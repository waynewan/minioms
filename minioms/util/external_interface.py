# --
# --
# --
__mktprc_loader__ = None
def set_mktprc_loader(loader):
	global __mktprc_loader__
	__mktprc_loader__ = loader

def mktprc_loader():
	if(__mktprc_loader__ is None):
		raise "ERR: __mktprc_loader__ not set"
	return __mktprc_loader__

