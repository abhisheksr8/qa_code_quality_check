from setuptools import setup, find_packages
setup(
    name = 'Python_E2E_Existing_Pipeline_All_2',
    version = '1.0',
    packages = find_packages(include = ('python_e2e_existing_pipeline_all_2*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.38'],
    entry_points = {
'console_scripts' : [
'main = python_e2e_existing_pipeline_all_2.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
