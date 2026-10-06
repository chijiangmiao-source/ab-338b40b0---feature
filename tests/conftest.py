import os
import tempfile

# Keep the module-level app instance (created on import of app.main) away
# from the repository working tree.
os.environ.setdefault(
    "DATABASE_PATH",
    os.path.join(tempfile.mkdtemp(prefix="rad-test-"), "radiation.db"),
)
