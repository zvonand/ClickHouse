#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>

#include <unistd.h> /// for ::getpid

#include <algorithm> /// for std::sort
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <vector>

namespace fs = std::filesystem;

namespace
{

/// Build a LocalObjectStorage rooted at `key_prefix` (directory must exist).
DB::ObjectStoragePtr makeLocalObjectStorage(const std::string & key_prefix)
{
    DB::LocalObjectStorageSettings settings(
        /*disk_name_=*/"test_local",
        /*key_prefix_=*/key_prefix,
        /*read_only_=*/false);
    return std::make_shared<DB::LocalObjectStorage>(std::move(settings));
}

/// A scoped temp directory that cleans itself up on destruction.
struct ScopedTempDir
{
    fs::path path;
    explicit ScopedTempDir(const std::string & name_hint)
        : path(fs::temp_directory_path() / fs::path(name_hint + "_" + std::to_string(::getpid())))
    {
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::create_directories(path);
    }
    ~ScopedTempDir()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

}


/// `LocalObjectStorage::listObjects` must not follow symlinks into a cycle
/// (otherwise it can blow the stack via unbounded recursion). This test
/// creates `<root>/a -> <root>` which would cycle forever if symlinks are
/// followed, then verifies that `listObjects` terminates and returns only
/// the real files on disk.
TEST(LocalObjectStorage, ListObjectsDoesNotFollowSymlinkCycles)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_cycle");
    const auto & root = tmp.path;

    /// Lay out:
    ///   root/
    ///     real.txt            ← regular file
    ///     sub/nested.txt      ← regular file at depth 1
    ///     loop -> .           ← symlink cycle targeting root
    ///     sub/back -> ..      ← another symlink cycle targeting root
    fs::create_directories(root / "sub");

    std::ofstream(root / "real.txt") << "hello";
    std::ofstream(root / "sub" / "nested.txt") << "world";

    std::error_code ec;
    fs::create_symlink(".", root / "loop", ec);
    ASSERT_FALSE(ec) << "Failed to create symlink `loop`: " << ec.message();
    fs::create_symlink("..", root / "sub" / "back", ec);
    ASSERT_FALSE(ec) << "Failed to create symlink `sub/back`: " << ec.message();

    auto storage = makeLocalObjectStorage(root.string());

    DB::RelativePathsWithMetadata children;
    /// Must return (not recurse forever / crash with stack overflow).
    storage->listObjects(root.string(), children, /* max_keys */ 0);

    std::vector<std::string> paths;
    paths.reserve(children.size());
    for (const auto & c : children)
        paths.push_back(c->relative_path);
    std::sort(paths.begin(), paths.end());

    /// Only the real files should be reported; the symlinks to directories
    /// are skipped (the default `recursive_directory_iterator` behaviour).
    ASSERT_EQ(paths.size(), 2u) << "Unexpected listing: {"
        << (paths.empty() ? std::string() : paths[0])
        << (paths.size() > 1 ? (std::string(", ") + paths[1]) : std::string()) << "}";
    EXPECT_EQ(paths[0], (root / "real.txt").string());
    EXPECT_EQ(paths[1], (root / "sub" / "nested.txt").string());
}

/// Sanity: a plain nested tree without symlinks should still be listed
/// exhaustively (i.e. the fix must not regress the common case).
TEST(LocalObjectStorage, ListObjectsWalksNestedDirectoriesWithoutSymlinks)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_nested");
    const auto & root = tmp.path;

    fs::create_directories(root / "a" / "b" / "c");
    std::ofstream(root / "top.txt") << "0";
    std::ofstream(root / "a" / "a_file.txt") << "1";
    std::ofstream(root / "a" / "b" / "b_file.txt") << "2";
    std::ofstream(root / "a" / "b" / "c" / "c_file.txt") << "3";

    auto storage = makeLocalObjectStorage(root.string());

    DB::RelativePathsWithMetadata children;
    storage->listObjects(root.string(), children, /* max_keys */ 0);

    std::vector<std::string> paths;
    paths.reserve(children.size());
    for (const auto & c : children)
        paths.push_back(c->relative_path);
    std::sort(paths.begin(), paths.end());

    ASSERT_EQ(paths.size(), 4u);
    EXPECT_EQ(paths[0], (root / "a" / "a_file.txt").string());
    EXPECT_EQ(paths[1], (root / "a" / "b" / "b_file.txt").string());
    EXPECT_EQ(paths[2], (root / "a" / "b" / "c" / "c_file.txt").string());
    EXPECT_EQ(paths[3], (root / "top.txt").string());
}

/// A non-existent or non-directory input must return an empty listing,
/// never throw or crash.
TEST(LocalObjectStorage, ListObjectsHandlesMissingAndNonDirectoryPaths)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_missing");
    const auto & root = tmp.path;

    auto storage = makeLocalObjectStorage(root.string());

    /// Missing path.
    {
        DB::RelativePathsWithMetadata children;
        storage->listObjects((root / "does_not_exist").string(), children, /* max_keys */ 0);
        EXPECT_TRUE(children.empty());
    }

    /// Regular file (not a directory).
    {
        const auto file_path = root / "just_a_file.txt";
        std::ofstream(file_path) << "x";
        DB::RelativePathsWithMetadata children;
        storage->listObjects(file_path.string(), children, /* max_keys */ 0);
        EXPECT_TRUE(children.empty());
    }
}
