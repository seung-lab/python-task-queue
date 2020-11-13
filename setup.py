import setuptools

setuptools.setup(
  setup_requires=['pbr'],
  include_package_data=True,
  entry_points={
    "console_scripts": [
      "ptq=taskqueue_cli:main"
    ],
  },
  long_description_content_type="text/markdown",
  pbr=True
)