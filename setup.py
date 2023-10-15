import os
from setuptools import find_packages, setup


with open("README.md", "r") as fh:
    long_desc = fh.read()


def required(requirements_file: str) -> None:
    base_dir = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(base_dir, requirements_file), "r") as f:
        requirements = f.read().splitlines()
        return [
            pkg
            for pkg in requirements
            if pkg.strip() and not pkg.startswith("#")
        ]


setup(
    name="alena_messagebus_client",
    packages=find_packages(),
    version="0.1.1",
    # install_requires=required("requirements.txt"),
    author="Alexander Belinsky",
    author_email="belinskyab@mail.ru",
    description="Alena MessageBus Client",
    long_description=long_desc,
    long_description_content_type="text/markdown",
)
