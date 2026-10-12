"""Check guest permissions in the real runtime image produced by the builder."""

import importlib.util
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "rootfs.py"
SPEC = importlib.util.spec_from_file_location("adx_runtime_rootfs", MODULE_PATH)
rootfs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rootfs)


class RuntimeRootfsPermissionsTest(unittest.TestCase):
    def test_real_image_root_is_traversable_and_owned_by_guest_root(self):
        # This is an explicit build gate: missing native tools must fail.
        for name in ("busybox", "readelf", "mkfs.erofs", "fsck.erofs", "dump.erofs"):
            self.assertIsNotNone(shutil.which(name), f"required tool missing: {name}")
        busybox = Path(shutil.which("busybox"))
        with tempfile.TemporaryDirectory(prefix="adx-rootfs-permissions-") as work:
            image = Path(work) / "runtime.img"
            rootfs.build(busybox, image, busybox=busybox)
            superblock = subprocess.check_output(
                ["dump.erofs", "-s", str(image)], text=True
            )
            nid = re.search(r"Filesystem root nid:\s*(\d+)", superblock)
            self.assertIsNotNone(nid, superblock)
            inode = subprocess.check_output(
                ["dump.erofs", "--nid=" + nid.group(1), str(image)], text=True
            )
            permissions = re.search(
                r"Uid:\s*(\d+)\s+Gid:\s*(\d+)\s+Access:\s*([0-7]+)", inode
            )
            self.assertIsNotNone(permissions, inode)
            self.assertEqual(
                tuple(int(value, 8 if index == 2 else 10)
                      for index, value in enumerate(permissions.groups())),
                (0, 0, 0o755),
                "The build user's private temporary directory must not become "
                "an inaccessible guest root: " + inode,
            )


if __name__ == "__main__":
    unittest.main()
