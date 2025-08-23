from setuptools import setup
setup(name='minioms',
version='0.1.0.dev1',
description='mini OMS',
url='#',
author='#',
author_email='#',
license='Apache 2.0',
packages=[
	'minioms',
	'minioms.obj',
	'minioms.oms_db',
	'minioms.util',
],
package_dir={
	'minioms'        : 'minioms',
	'minioms.obj'    : 'minioms/obj',
	'minioms.oms_db' : 'minioms/oms_db',
	'minioms.util'   : 'minioms/util',
},
zip_safe=False)
