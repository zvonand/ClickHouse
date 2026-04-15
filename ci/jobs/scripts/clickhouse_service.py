import shutil
import subprocess
import time
import urllib.request
from pathlib import Path

from ci.praktika.service import Service
from ci.praktika.utils import Shell, Utils

repo_dir = Utils.cwd()
temp_dir = f"{repo_dir}/ci/tmp"


class ClickHouseService(Service):
    def __init__(
        self,
        ch_config_dir: str = f"{temp_dir}/etc/clickhouse-server",
        ch_var_lib_dir: str = f"{temp_dir}/var/lib/clickhouse",
        run_path: str = f"{temp_dir}/run",
    ):
        self.ch_config_dir = ch_config_dir
        self.ch_var_lib_dir = ch_var_lib_dir
        self.run_path = run_path
        self.config_file = f"{ch_config_dir}/config.xml"
        self.pid_file = f"{ch_config_dir}/clickhouse-server.pid"
        self.log_dir = f"{temp_dir}/var/log/clickhouse-server"
        self.user_files_path = f"{run_path}/user_files"
        self._proc = None
        self._log_fd = None

    @staticmethod
    def _download_binary() -> bool:
        dest = Path(temp_dir) / "clickhouse"
        if dest.exists():
            print(f"ClickHouse binary already present at [{dest}], skipping download")
            dest.chmod(0o755)
            return True
        arch = "aarch64" if Utils.is_arm() else "amd64"
        url = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/{arch}/clickhouse"
        print(f"Downloading ClickHouse binary from [{url}] to [{dest}]")
        try:
            urllib.request.urlretrieve(url, dest)
            dest.chmod(0o755)
            return True
        except Exception as e:
            print(f"ERROR: failed to download ClickHouse binary: {e}")
            return False

    def _print_server_log(self) -> None:
        log_path = Path(self.log_dir) / "clickhouse-server.log"
        if self._log_fd is not None:
            self._log_fd.flush()
        if log_path.exists():
            print(f"--- {log_path} ---")
            print(log_path.read_text(errors="replace")[-4096:])
            print("--- end ---")

    def _wait_ready(self, port: int = 9000, attempts: int = 30, delay: int = 2) -> bool:
        # Wait for the pid file to appear, bail out early if the process exits
        pid = None
        for _ in range(30):
            if self._proc and self._proc.poll() is not None:
                print(f"Server process exited with code {self._proc.returncode}")
                self._print_server_log()
                return False
            try:
                pid = int(Path(self.pid_file).read_text().strip())
                break
            except Exception:
                time.sleep(1)
        if pid is None:
            print(f"Failed to get PID from [{self.pid_file}]")
            self._print_server_log()
            return False

        for attempt in range(attempts):
            _res, out, err = Shell.get_res_stdout_stderr(
                f'clickhouse-client --port {port} --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("ClickHouse server ready")
                return True
            print(f"Server not ready (attempt {attempt + 1}/{attempts}), err: {err}")
            time.sleep(delay)
            if self._proc and self._proc.poll() is not None:
                print(f"Server process exited with code {self._proc.returncode}")
                self._print_server_log()
                return False
        print(f"Server not ready after {attempts * delay}s")
        self._print_server_log()
        return False

    def start(self) -> bool:
        Utils.add_to_PATH(temp_dir)

        # Download binary if absent
        clickhouse_bin = Path(temp_dir) / "clickhouse"
        if not clickhouse_bin.exists():
            if not self._download_binary():
                print("Failed to download ClickHouse binary")
                return False

        # Create symlinks if absent
        for link_name in ("clickhouse-server", "clickhouse-client", "clickhouse-local"):
            link_path = Path(temp_dir) / link_name
            if not link_path.exists():
                Utils.link(clickhouse_bin, link_path)

        # Copy server config files if absent
        config_dir = Path(self.ch_config_dir)
        if not (config_dir / "config.xml").exists():
            config_dir.mkdir(parents=True, exist_ok=True)
            src_dir = Path("./programs/server")
            for name in ("config.xml", "users.xml"):
                shutil.copy(src_dir / name, config_dir / name)
            shutil.copytree(
                src_dir / "config.d",
                config_dir / "config.d",
                symlinks=False,
                dirs_exist_ok=True,
            )

        # Prepare directories
        Path(self.run_path).mkdir(parents=True, exist_ok=True)
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        Path(self.pid_file).unlink(missing_ok=True)

        command = (
            f"clickhouse-server --config-file {self.config_file}"
            f" --pid-file {self.pid_file}"
            f" -- --path {self.run_path}"
            f" --user_files_path {self.user_files_path}"
            f" --top_level_domains_path {self.ch_config_dir}/top_level_domains"
            f" --logger.stderr {self.log_dir}/stderr.log"
        )
        print(f"Starting ClickHouse server: {command}")
        self._log_fd = open(f"{self.log_dir}/clickhouse-server.log", "w")
        self._proc = subprocess.Popen(
            command,
            stderr=subprocess.STDOUT,
            stdout=self._log_fd,
            shell=True,
            cwd=self.run_path,
        )

        return self._wait_ready()

    def shutdown(self) -> bool:
        if self._proc is None:
            return True
        self._proc.terminate()
        try:
            self._proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait()
        if self._log_fd is not None:
            self._log_fd.close()
            self._log_fd = None
        return True
