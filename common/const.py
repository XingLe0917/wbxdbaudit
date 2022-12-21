class _const:
    class ConstError(TypeError): pass
    class ConstCaseError(ConstError): pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError("Can't change const value!")
        if not name.isupper():
            raise self.ConstCaseError('const "%s" is not all letters are capitalized' % name)
        self.__dict__[name] = value

# import sys
# sys.modules[__name__] = _const()

const = _const()
const.APPLN_SUPPORT_CODE_CONFIGDB = "CONFIG"
const.APPLN_SUPPORT_CODE_WEBDB = "WEB"
const.APPLN_SUPPORT_CODE_TAHOEDB = "TEL"
const.APPLN_SUPPORT_CODE_TEODB = "TEO"
const.APPLN_SUPPORT_CODE_OPDB = "OPDB"
const.APPLN_SUPPORT_CODE_GLOOKUPDB = "LOOKUP"
const.APPLN_SUPPORT_CODE_MEDIATEDB = "MEDIATE"

const.SCHEMATYPE_APP = "app"
const.SCHEMATYPE_WBXMAINT = "wbxmaint"
const.SCHEMATYPE_GLOOKUP = "glookup"
const.SCHEMATYPE_XXRPTH = "xxrpth"
const.SCHEMATYPE_TEST = "test"
const.SCHEMATYPE_COLLABMED = "collabmed"
