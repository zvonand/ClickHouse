#!/usr/bin/env python3

import argparse
from pathlib import Path
from typing import Tuple
import subprocess
import logging
import sys
import time

from ci_config import CI_CONFIG, BuildConfig
from cache_utils import CargoCache

from env_helper import (
    GITHUB_RUN_ID,
    REPO_COPY,
    S3_ACCESS_KEY_ID,
    S3_BUILDS_BUCKET,
    S3_SECRET_ACCESS_KEY,
    TEMP_PATH,
)
from git_helper import Git
from pr_info import PRInfo, EventType
from report import FAILURE, JobReport, StatusType, SUCCESS
from s3_helper import S3Helper
from tee_popen import TeePopen
import docker_images_helper
from version_helper import (
    ClickHouseVersion,
    VersionType,
    get_version_from_repo,
    update_version_local,
    get_version_from_tag,
)
from stopwatch import Stopwatch

IMAGE_NAME = "altinityinfra/binary-builder"
BUILD_LOG_NAME = "build_log.log"


def _can_export_binaries(build_config: BuildConfig) -> bool:
    if build_config.package_type != "deb":
        return False
    if build_config.sanitizer != "":
        return True
    if build_config.debug_build:
        return True
    return False


def get_packager_cmd(
    build_config: BuildConfig,
    packager_path: Path,
    output_path: Path,
    cargo_cache_dir: Path,
    build_version: str,
    image_version: str,
    official: bool,
) -> str:
    package_type = build_config.package_type
    comp = build_config.compiler
    cmake_flags = "-DENABLE_CLICKHOUSE_SELF_EXTRACTING=1"
    cmd = (
        f"cd {packager_path} && CMAKE_FLAGS='{cmake_flags}' ./packager "
        f"--output-dir={output_path} --package-type={package_type} --compiler={comp}"
    )

    if build_config.debug_build:
        cmd += " --debug-build"
    if build_config.sanitizer:
        cmd += f" --sanitizer={build_config.sanitizer}"
    if build_config.coverage:
        cmd += " --coverage"
    if build_config.tidy:
        cmd += " --clang-tidy"

    cmd += " --cache=sccache"
    cmd += " --s3-rw-access"
    cmd += f" --s3-bucket={S3_BUILDS_BUCKET}"
    cmd += f" --s3-access-key-id={S3_ACCESS_KEY_ID}"
    cmd += f" --s3-secret-access-key={S3_SECRET_ACCESS_KEY}"
    cmd += f" --cargo-cache-dir={cargo_cache_dir}"

    if build_config.additional_pkgs:
        cmd += " --additional-pkgs"

    cmd += f" --docker-image-version={image_version}"
    cmd += " --with-profiler"
    cmd += f" --version={build_version}"

    if _can_export_binaries(build_config):
        cmd += " --with-binaries=tests"

    if official:
        cmd += " --official"

    return cmd


def build_clickhouse(
    packager_cmd: str, logs_path: Path, build_output_path: Path
) -> Tuple[Path, StatusType]:
    build_log_path = logs_path / BUILD_LOG_NAME
    success = False
    with TeePopen(packager_cmd, build_log_path) as process:
        retcode = process.wait()
        if build_output_path.exists():
            results_exists = any(build_output_path.iterdir())
        else:
            results_exists = False

        if retcode == 0:
            if results_exists:
                success = True
                logging.info("Built successfully")
            else:
                logging.info(
                    "Success exit code, but no build artifacts => build failed"
                )
        else:
            logging.info("Build failed")
    return build_log_path, SUCCESS if success else FAILURE


def get_release_or_pr(pr_info: PRInfo, version: ClickHouseVersion) -> Tuple[str, str]:
    "Return prefixes for S3 artifacts paths"
    # FIXME performance
    # performance builds are havily relies on a fixed path for artifacts, that's why
    # we need to preserve 0 for anything but PR number
    # It should be fixed in performance-comparison image eventually
    # For performance tests we always set PRs prefix
    performance_pr = "PRs/0"
    if pr_info.event_type == "dispatch":
        # for dispatch maintenance run we use major version and time
        return f"maintenance/{pr_info.base_ref}/{GITHUB_RUN_ID}", performance_pr
    if "release" in pr_info.labels or "release-lts" in pr_info.labels:
        # for release pull requests we use branch names prefixes, not pr numbers
        return pr_info.head_ref, performance_pr
    if pr_info.number == 0:
        # for pushes to master - major version
        return f"{version.major}.{version.minor}", performance_pr
    # PR number for anything else
    pr_number = f"PRs/{pr_info.number}"
    return pr_number, pr_number


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser("Clickhouse builder script")
    parser.add_argument(
        "build_name",
        help="build name",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    args = parse_args()

    stopwatch = Stopwatch()
    build_name = args.build_name

    build_config = CI_CONFIG.build_config[build_name]

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_path = Path(REPO_COPY)

    pr_info = PRInfo()

    logging.info("Repo copy path %s", repo_path)

    s3_helper = S3Helper()

    version = get_version_from_repo(git=Git(True))
    logging.info("Got version from repo %s", version.string)

    # official_flag = pr_info.number == 0

    # version_type = "testing"
    # if "release" in pr_info.labels or "release-lts" in pr_info.labels:
    #     version_type = "stable"
    #     official_flag = True

    # NOTE(vnemkov): For Altinity builds, version flavor is taken from autogenerated_versions.txt
    official_flag = True

    if pr_info.event_type == EventType.PUSH \
            and pr_info.ref.startswith('refs/tags/'):
        tag_name = pr_info.ref.removeprefix('refs/tags/')

        version_from_tag = get_version_from_tag(tag_name)

        # tag can override only `tweak` and `flavour`
        assert version_from_tag.major == version.major \
            and version_from_tag.minor == version.minor \
            and version_from_tag.patch == version.patch

        version._flavour = version_from_tag._flavour
        version.tweak = version_from_tag.tweak

        logging.info("Updated version info from tag: %s => %s", tag_name, version)

    # TODO(vnemkov): make sure tweak part is incremented by 1 each time we merge a PR
    update_version_local(version, version._flavour)

    logging.info(f"Updated local files with version : {version.string} / {version.describe}")

    logging.info("Build short name %s", build_name)

    build_output_path = temp_path / build_name
    build_output_path.mkdir(parents=True, exist_ok=True)
    cargo_cache = CargoCache(
        temp_path / "cargo_cache" / "registry", temp_path, s3_helper
    )
    cargo_cache.download()

    docker_image = docker_images_helper.get_docker_image(IMAGE_NAME)
    docker_image.version = "e0a138049b31"
    docker_image = docker_images_helper.pull_image(docker_image)

    packager_cmd = get_packager_cmd(
        build_config,
        repo_path / "docker" / "packager",
        build_output_path,
        cargo_cache.directory,
        version.string,
        docker_image.version,
        official_flag,
    )

    logging.info("Going to run packager with %s", packager_cmd)

    logs_path = temp_path / "build_log"
    logs_path.mkdir(parents=True, exist_ok=True)

    start = time.time()
    log_path, build_status = build_clickhouse(
        packager_cmd, logs_path, build_output_path
    )
    elapsed = int(time.time() - start)
    subprocess.check_call(
        f"sudo chown -R ubuntu:ubuntu {build_output_path}", shell=True
    )
    logging.info("Build finished as %s, log path %s", build_status, log_path)
    if build_status == SUCCESS:
        cargo_cache.upload()
    else:
        # We check if docker works, because if it's down, it's infrastructure
        try:
            subprocess.check_call("docker info", shell=True)
        except subprocess.CalledProcessError:
            logging.error(
                "The dockerd looks down, won't upload anything and generate report"
            )
            sys.exit(1)

    JobReport(
        description=version.describe,
        test_results=[],
        status=build_status,
        start_time=stopwatch.start_time_str,
        duration=elapsed,
        additional_files=[log_path],
        build_dir_for_upload=build_output_path,
        version=version.describe,
    ).dump()

    # Fail the build job if it didn't succeed
    if build_status != SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
