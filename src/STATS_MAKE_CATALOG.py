#/***********************************************************************
# * Licensed Materials - Property of IBM 
# *
# * IBM SPSS Products: Statistics Common
# *
# * (C) Copyright IBM Corp. 1989, 2024
# *
# * US Government Users Restricted Rights - Use, duplication or disclosure
# * restricted by GSA ADP Schedule Contract with IBM Corp. 
# ************************************************************************/

# Construct a dataset listing the variables and selected properties for a collection of data files


# 05-23-2008 Original version - JKP
# 04-29-2009 Add file handle support
# 11-16-2009 Protect against UP converting escape sequences with "\" characters
# 12-16-2009 Enable translation
# 10-03-2022 Rename and add variable filter
# 06-22-2024 Major rewrite to eliminate use of Dataset class

__version__ = "1.4.1"
__author__ = "JKP, SPSS"

# debugging
        # makes debug apply only to the current thread
try:
    import wingdbstub
    import threading
    wingdbstub.Ensure()
    wingdbstub.debugger.SetDebugThreads({threading.get_ident(): 1})
except:
    pass

import spss, os, re
import spss, spssaux, spssdata
from collections import defaultdict
import random
from extension import Template, Syntax
from extension import processcmd
    
maindsname = "D" + str(random.uniform(.05, 1))
omsendname = "O" + str(random.uniform(.05, 1))
omsvltag = "V" + str(random.uniform(.05, 1))


def gather(dsname, files, filetypes=["spss"], filenamepattern=None,attrlist=[], attrlength=256,
        varnamepattern=None, valuelabels=False):
    """Create SPSS dataset listing variable names, variable labels, and source files for selected files.  Return the name of the new dataset.
    
    files is a list of files and/or directories.  If an item is a file, it is processed; if it is a directory, the files and subdirectories
    it contains are processed.
    filetypes is a list of filetypes to process.  It defaults to ["spss"] which covers sav and por.  It can also include
    "sas" for sas7bdat, sd7, sd2, ssd01, and xpt, and "stata" for dta
    filenamepattern is an optional parameter that can contain a regular expression to be applied to the filenames to filter the
    datasets that are processed.  It is applied to the filename itself, omitting any directory path and file extension.  The expression
    is anchored to the start of the name and ignores case.
    dsname is an optional name to be assigned to the new dataset.  If not specified, a name will be automatically generated.
    If dsname is specified, it will become the active dataset; otherwise, it need not be the active dataset.
    attrlist is an optional list of custom attributes to be included in the output. For array attributes, only the first item is
    recorded.  The value is blank if the attribute is not present for the variable.  Attribute variables are
    strings of size attrlength bytes, truncated appropriately.
    
    The output is just a dataset.  It must be saved, if desired, after this function has completed.
    Its name is the return value of this function.
    Exception is raised if any files not found.
    
    Examples:
    gathermetadata.gather(["c:/temp/firstlevel", "c:/spss16/samples/voter.sav"], ["spss", "sas"])
    searches spss and sas files in or under the temp/firstlevel directory plus the voter file.
    
    gathermetadata.gather(["c:/temp/firstlevel"], filenamepattern="car")
    searches the firstlevel directory for spss files whose names start with "car".
    """
    
    ###filetypes = [f.lower() for f in filetypes]

        
    # catalog accumulates all the information for the catalog dataset
    # entries are filename, varname, varlabel, optionally value label count and value label string,
    # and attributes
    catalog = []
    filecount = 0
            
    
    files = [fixescapes(f) for f in files]  #UP is converting escape characters :-)
    # walk the list of files and directories and open
    

    fh = spssaux.FileHandles()
    
    notfound = []
    for item in files:
        try:
            item = fh.resolve(item)
        except:
            pass
        if os.path.isfile(item):
            filecount = addvarinfo(catalog, item, filetypes, filenamepattern, attrlist, attrlength, filecount, varnamepattern, valuelabels)
        elif os.path.isdir(item): 
            for dirpath, dirnames, fnames in os.walk(item):
                for f in fnames:
                    try:
                        filecount = addvarinfo(catalog, os.path.join(dirpath, f),
                        filetypes, filenamepattern, attrlist, attrlength, filecount, varnamepattern, valuelabels)
                    except EnvironmentError as e:
                        notfound.append(e.args[0])
        else:
            if not isinstance(item, str):
                item = str(item)
            notfound.append(_("Not found: %s") % item)

    # make dataset from catalog list
    makedataset(catalog, dsname, valuelabels, attrlist)
    print(_(f"""*** Files processed: {filecount}"""))
    
def addvarinfo(catalog, filespec, filetypes, filenamepattern, attrlist, attrlength, filecount, 
        varnamepattern=None, valuelabels=None):
    """add variable information to a dataset.
    
    catalog is the  list of records to append to.
    filetypes is the list of file types to include.
    filenamepattern is a regular expression to filter filename roots.
    dsvars is a special dictionary of variables and attributes.  See function addunique.
    attrindexes is a dictionary with keys of lower case attribute names and values as the dataset index starting with 0.
    attrlength is the size of the attribute string variables
    valuelabels indicates whether or not to include value label information"""

    ftdict = {"spss":[".sav", ".zsav"], 
        "spsspor": [".por"], 
        "sas":[".sas7bdat",".sd7",".sd2",".ssd01",".ssd04", ".xpt"], "stata":[".dta"]}
    spsscmd = {"spss":"""GET FILE="%s." """, 
        "spsspor": """IMPORT FILE="%s." """,
        "sas": """GET SAS DATA="%s." """, "stata": """GET STATA FILE="%s." """}
    if filenamepattern:
        try:
            pat = re.compile(filenamepattern, re.IGNORECASE)
        except:
            raise ValueError(_("Invalid file name pattern: %s") % filenamepattern)
    else:
        pat = None
        
    if varnamepattern:
        try:
            vpat = re.compile(varnamepattern, re.IGNORECASE)
        except:
            raise ValueError(_("Invalid variable name pattern: %s") % varnamepattern)
    else:
        vpat = None
    filecount = addinfo(filespec, filetypes, ftdict, spsscmd, catalog, valuelabels, attrlist, filecount, pat, vpat)
    return filecount

def addinfo(filespec, filetypes, ftdict, spsscmd, catalog, valuelabels, attrlist, filecount, pat=None, vpat=None):
    """open the file if appropriate type, extract variable information, and add it to catalog.
    
    filespec is the file to open
    dsname is the dataset name to append to
    filetypes is the list of file types to include."""
    
    fnsplit = os.path.split(filespec)[1]
    fn, ext = os.path.splitext(fnsplit)
    ext = ext.lower()
    havedsname = False

    for ft in filetypes:
        if ext in ftdict[ft]:
            if pat is None or pat.match(fn):
                try:
                    spss.Submit(spsscmd[ft] % filespec)
                    spss.Submit(f"DATASET NAME {maindsname}.")
                    havedsname = True
                except:
                    raise EnvironmentError(_("File could not be opened, skipping: %s") % filespec)
            else:
                continue
        else:
            continue
        filecount = filecount + 1
        spss.Submit(f"""oms select all except = texts /destination viewer=no /tag ={omsendname}.""")
        # get value labels first.  Otherwise vardict will reflect the wrong dataset.
        if valuelabels:
            vldict = getvaluelabels()
        spss.Submit(f"DATASET ACTIVATE {maindsname}.")
        if vpat:
            vardict = spssaux.VariableDict(pattern=vpat, caseless=True)
        else:
            vardict = spssaux.VariableDict(caseless=True)
        try:
            for v in vardict:
                try:
                    record = ([filespec, v.VariableName, v.VariableLabel])
                    if valuelabels:
                        try:                            
                            record.append(len(vldict[v.VariableName]))  # number of value labels
                            record.append(";".join(vldict[v.VariableName])) # concatinated labels
                        except:
                            record.append(0)
                            record.append("")
                except:
                    print(f"""Bad character in file {filespec}, variable {v.VariableName} in variable name or label""")                    
    
                if attrlist:
                    attrs = getattributes(vardict, v.VariableName, attrlist)
                    record.extend(attrs)
                catalog.append(record)
        finally:
            spss.Submit(f"""omsend tag=["{omsendname}"].""")
            
    if havedsname:
        spss.Submit(f"DATASET CLOSE {maindsname}.")
    return filecount

def getvaluelabels():
    """return value labels dict for current active file
    
    key = varname
    value is the value label"""
    
    # uses DISPLAY DICT and OMS due to performance problems with Dataset apis
    # DISPLAY DICT does not produce a value labels row if a variable has no labels
    # The filename parameter is just for later matching purposes
    
    spss.Submit(f"""dataset declare vls.
    oms select tables /if subtypes='Variable Values'
        /destination outfile=vls format=sav viewer=no
        /tag = {omsvltag}.
    display dict.
    omsend tag=["{omsvltag}"].""")

    # Sometimes OMS leaves an empty dataset missing some variable names
    # and sometimes it omits the dataset altoghether when there are no value labels
    try:
        spss.Submit("DATASET ACTIVATE vls")
    except:   # no value labels
        return {}
        
    d = defaultdict(list)
    try:
        vls = spssdata.Spssdata("Var1 Label", names=False).fetchall()
        for item in vls:
            d[item[0].rstrip()].append(item[1].rstrip())
    except:
        pass
    spss.Submit(f"""DATASET CLOSE vls.""")
    return d
      
def getattributes(vardict, varname, attrlist):
    """return list of custom attribute values named in attrlist
    
    returned value is blank if attribute is  not present
    vardict is a VariableDict object
    varname is the variable name (not case sensitive)
    attrlist is a list of attribute names
    """
    
    # sometimes there are no attributes, and we want to suppress the message
    # but that would be too many OMS invocations
    attrs = []
    anames = [aname.lower() for aname in attrlist]
    vattr = {k.lower(): v for (k, v) in vardict[varname].Attributes.items()}
    for a in anames:
        attrs.append(str(vattr.get(a, "")))
    return attrs
    
    
#def addunique(dsdict, key):
    #"""Add modified version of key to dictionary dsdict.  Return generated key.
    
    #dsdict is a dictionary whose keys will be lower case strings and whose values are unique SPSS variable names.
    #duplicate keys are ignored.
    #keys are automatically prefixed with "*" to separate them from variable names that could be identical."""
    
    #key1 = "*" + key.lower()
    #if key1 in dsdict:
        #return key1

    ## make a version of key that is unique in the dictionary values and a legal variable name length
    #i=0
    #keymod = spssaux.truncatestring(key, 64)
    #while keymod.lower() in [k.lower() for k in list(dsdict.values())]:
        #keymod = spssaux.truncatestring(key, 59) + "_" + str(i)
        #i += 1
    #dsdict[key1] = keymod
    #return key1

escapelist = [('\a', r'\a'), ('\b', r'\b'), ('\f', r'\f'), ('\n', r'\n'), ('\r', r'\r'), ('\t',r'\t'),('\v', r'\v')]

def fixescapes(item):
    return(re.sub(r"\\", "/", item))


def makedataset(catalog, dsname, valuelabels, attrlist):
    """Create a dataset from the catalog
    
    catalog is a list of variable records.  Each record has
        file name, variable name, variable label and, optionally
        value label count and value label, and optionally attribute values for
        selected attributes
        
    dsname is the name for the dataset to be created
    valuelabels is a boolean for whether those labels are included
    attrlist is a list of attribute names
    
    If attribute names coincide with variables below, the command will fail."""
    
    fn = spssdata.vdef("source", vtype=255, vlabel=_("File Containing the Variable"))
    vn = spssdata.vdef("variableName", vtype=64, vlabel=_("Variable Name"))
    vl =spssdata.vdef("variableLabel", vtype=256)
    if valuelabels:
        vllen = spssdata.vdef("NvalueLabels", vtype=0, vlabel=_("Number of Value Labels"))
        vls = spssdata.vdef("ValueLabels", vtype=1000, vlabel = _("Value Labels"))
    if attrlist:
        # calculate required length of attribute strings across all selected attributes
        maxalen = 0
        for row in catalog:
            for a in row[-len(attrlist):]:
                maxalen = max(max([maxalen], [len(item) for item in [a]]))
        maxalen = max(maxalen, 1)
        attrvars = []
        for attr in attrlist:
            # assume character expansion to utf-8 no worse than 1.5 over entire string
            attrvars.append(spssdata.vdef(attr, int(maxalen * 1.5), _("Attribute")))
    
    curs = spssdata.Spssdata(accessType="n")
    curs.append(fn)
    curs.append(vn)
    curs.append(vl)
    if valuelabels:
        curs.append(vllen)
        curs.append(vls)
    if attrlist:
        for a in attrvars:
            curs.append(a)
    curs.commitdict()
    
    for item in catalog:
        curs.appendvalue("source", item[0])
        curs.appendvalue("variableName", item[1])
        curs.appendvalue("variableLabel", item[2])
        if valuelabels:
            curs.appendvalue("NvalueLabels", item[3])
            curs.appendvalue("ValueLabels", item[4])
        if attrlist:
            for i, a in enumerate(attrlist):
                curs.appendvalue(a, item[3 + i + (valuelabels and 2 or 0)])
        curs.CommitCase()
    curs.CClose()
    spss.Submit(f"""DATASET NAME {dsname}""")


def Run(args):
    """Execute the GATHERMD command"""
    
    
    ###print args   #debug
    args = args[list(args.keys())[0]]
    
    r"""GATHERMD
    Create and activate a dataset whose cases are variable names and labels 
    and, optionally, selected attributes from one or more data files.
    
    GATHERMD list-of-specifications
    [/OPTIONS [FILETYPES=*spss sas stata]
    [DSNAME=name]
    [FILENAMEPATTERN="pattern expression"]]
    [ATTRLENGTH=value]
    [/ATTRIBUTES list-of-attribute-names]
    
    [HELP].
    
    list-of-specifications is a list of one or more filenames, optionally with paths, and/or directories.  
    For directories, all appropriate files in the directory and its subdirectories are searched.  With version 18
    or later, the file specifications can include PASW Statistics file handles.
    
    FILETYPES defaults to SPSS files (.sav and .por).
    sas files are .sas7bdat, .sd7, .sd2, .ssd01, and .xpt
    stata files are .dta
    
    Files with any of the specified types found in the directories specified are searched.  Since 
    these files are opened in SPSS, if the same file is already open in SPSS, it will be reopened 
    without saving any changes that may have been made.
    
    DSNAME optionally specifies a dataset name to be assigned to the output dataset.
    
    FILENAMEPATTERN can be specified as a quoted literal containing a regular expression pattern
    to be used as a filter on filenames.  For example, FILENAMEPATTERN="car" would limit the 
    files searched to those whose name starts with "car".  FILENAMEPATTERN=".*car" would accept 
    any filenames containing "car".  These are not the same as filename wildcards found in many operating systems.
    For example, "abc*" will match any name starting with ab: it means literally ab followed by zero or more c's.
    The regular expression is not case sensitive, and it is applied to the name of the 
    file without the extension.  For a full explanation of regular expressions, one good source is
    http://www.amk.ca/python/howto/regex/
    
    /ATTRIBUTES list-of-names
    specifies a list of custom variable attributes to be included in the output dataset.  The variable 
    names will be the attribute names except if they conflict with the built-in variables source, 
    VariableName, and VariableLabel. If the attribute is not present, the value will be blank.  
    If the attribute is an array, only the first value is included.
    Attribute variables in the output dataset are truncated to the length specified in ATTRLENGTH, 
    which defaults to 256 
    
    /HELP displays this text and does nothing else.
    
    Examples:
    
    GATHERMD "c:/spss17/samples".
    
    gathermd "c:/temp/firstlevel" "c:/spss16/samples/voter.sav" /options filetypes=spss sas
    dsname=gathered.
    """
    
    oobj = Syntax([
        Template("", subc="", var="files", ktype="literal", islist=True),
        
        Template("FILETYPES", subc="OPTIONS", var="filetypes", ktype="str", 
                 vallist = ["spss", "sas", "stata", "spsspor"], islist=True),
        Template("FILENAMEPATTERN", subc="OPTIONS", var="filenamepattern",  ktype="literal"),
        Template("DSNAME", subc="OPTIONS", var="dsname", ktype="varname"),
        Template("ATTRLENGTH", subc="OPTIONS", var="attrlength", ktype="int", vallist=(1, 32767)),
        Template("VALUELABELS", subc="OPTIONS", var="valuelabels", ktype="bool"),
        
        Template("", subc="ATTRIBUTES", var="attrlist", ktype="varname", islist=True),
        Template("VARNAMEPATTERN", subc="OPTIONS", var="varnamepattern", ktype="literal")
    ])
    
    global _
    try:
        _("---")
    except:
        def _(msg):
            return msg

    if "HELP" in args:
        #print helptext
        helper()
    else:
        processcmd(oobj, args, gather)

def helper():
    """open html help in default browser window
    
    The location is computed from the current module name"""
    
    import webbrowser, os.path
    
    path = os.path.splitext(__file__)[0]
    helpspec = "file://" + path + os.path.sep + \
         "markdown.html"
    
    # webbrowser.open seems not to work well
    browser = webbrowser.get()
    if not browser.open_new(helpspec):
        print(("Help file not found:" + helpspec))
try:    #override
    from extension import helper
except:
    pass

