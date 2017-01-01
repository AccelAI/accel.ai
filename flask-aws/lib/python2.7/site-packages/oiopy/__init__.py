import pkg_resources

try:
    __version__ = __canonical_version__ = pkg_resources.get_provider(
        pkg_resources.Requirement.parse('oiopy')).version
except pkg_resources.DistributionNotFound:

    import pbr.version
    _version_info = pbr.version.VersionInfo('oiopy')
    __version__ = _version_info.release_string()
    __canonical_version__ = _version_info.version_string()
