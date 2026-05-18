"""Server shim — revision publishing lives in ``scripts.templates.template_versions_v1``."""

from scripts.templates.template_versions_v1 import (  # noqa: F401
    MANIFEST_VERSION_V1,
    VersionPolicy,
    activate_template_revision,
    has_legacy_flat,
    has_versioned_package,
    is_published_on_disk,
    legacy_flat_path,
    list_template_versions,
    load_active_template_doc,
    load_all_active_template_docs,
    manifest_path,
    publish_template_versioned,
    revision_doc_path,
    template_package_dir,
    unpublish_template,
)
