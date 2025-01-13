from setuptools import find_packages, setup

package_name = 'hyveos'

setup(
    name=package_name,
    version='0.1.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Josef Zoller',
    maintainer_email='josef@walterzollerpiano.com',
    description='The HyveOS ROS 2 bridge',
    license='MIT',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'bridge = hyveos.bridge:main',
        ],
    },
)
