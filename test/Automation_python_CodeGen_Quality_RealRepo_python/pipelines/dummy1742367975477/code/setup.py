from setuptools import setup, find_packages
setup(
    name = 'dummy1742367975477',
    version = '1.0',
    packages = find_packages(include = ('dummy1742367975477*', )) + ['prophecy_config_instances.dummy1742367975477'],
    package_dir = {'prophecy_config_instances.dummy1742367975477' : 'configs/resources/dummy1742367975477'},
    package_data = {'prophecy_config_instances.dummy1742367975477' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.38'],
    entry_points = {
'console_scripts' : [
'main = dummy1742367975477.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
