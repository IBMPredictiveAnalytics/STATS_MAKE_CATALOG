#/***********************************************************************
# * Licensed Materials - Property of IBM 
# *
# * IBM SPSS Products: Statistics Common
# *
# * (C) Copyright IBM Corp. 1989, 2022
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

__version__ = "1.3.0"
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

import spss, os, re, locale
import spssaux
from extension import Template, Syntax

from extension import processcmd

    
class DataStep(object):
    def __enter__(self):
        """initialization for with statement"""
        try:
            spss.StartDataStep()
        except:
            spss.Submit("EXECUTE")
            spss.StartDataStep()
        return self
    
    def __exit__(self, type, value, tb):
        spss.EndDataStep()
        return False

# The following block of code is for using the gather function as an Extension command.
def Run(args):
    """Execute the GATHERMD command"""
    
    
    ###print args   #debug
    args = args[list(args.keys())[0]]
    
    helptext=r"""GATHERMD
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
    
    
    # define the command syntax and enable translation
    
    oobj = Syntax([
        Template("", subc="", var="files", ktype="literal", islist=True),
        Template("FILETYPES", subc="OPTIONS", var="filetypes", ktype="str", 
                 vallist = ["spss", "sas", "stata", "spsspor"], islist=True),
        Template("FILENAMEPATTERN", subc="OPTIONS", var="filenamepattern",  ktype="literal"),
        Template("DSNAME", subc="OPTIONS", var="dsname", ktype="varname"),
        Template("ATTRLENGTH", subc="OPTIONS", var="attrlength", ktype="int", vallist=(1, 32767)),
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
def gather(files, filetypes=["spss"], filenamepattern=None, dsname=None,attrlist=[], attrlength=256,
        varnamepattern=None):
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
    
    encoding = locale.getlocale()[1]
    filetypes = [f.lower() for f in filetypes]
    dsvars = {"source":"source", "variablename":"VariableName", "variablelabel":"variableLabel"}
    
    with DataStep():
        ds = spss.Dataset(name=None)
        dsn = ds.name
        varlist = ds.varlist
        varlist.append("source",200)
        varlist["source"].label=_("File containing the variable")
        varlist.append("variableName", 64)
        varlist["variableName"].label = _("Variable Name")
        varlist.append("variableLabel", 256)
        varlist["variableLabel"].label  = _("Variable Label")

        attrindexes = {}
        for i, aname in enumerate(attrlist):
            anamemod = addunique(dsvars, aname)
            varlist.append(dsvars[anamemod], attrlength)
            attrindexes[aname.lower()] = i
            
        
    addvarinfo = makeaddinfo(dsn, filetypes, filenamepattern, dsvars, attrindexes, attrlength, varnamepattern)   #factory function
    
    files = [fixescapes(f) for f in files]  #UP is converting escape characters :-)
    # walk the list of files and directories and open
    
    try:   # will fail if spssaux is prior to version 2.3
        fh = spssaux.FileHandles()
    except:
        pass
    
    notfound = []
    for item in files:
        try:
            item = fh.resolve(item)
        except:
            pass
        if os.path.isfile(item):
            addvarinfo(item)
        elif os.path.isdir(item): 
            for dirpath, dirnames, fnames in os.walk(item):
                for f in fnames:
                    try:
                        addvarinfo(os.path.join(dirpath, f))
                    except EnvironmentError as e:
                        notfound.append(e.args[0])
        else:
            if not isinstance(item, str):
                item = str(item, encoding)
            notfound.append(_("Not found: %s") % item)

    spss.Submit("DATASET ACTIVATE %s." % dsn)
    if not dsname is None:
        spss.Submit("DATASET NAME %s." % dsname)
        dsn = dsname
    if notfound:
        raise ValueError("\n".join(notfound))
    return dsn
    
def makeaddinfo(dsname, filetypes, filenamepattern, dsvars, attrindexes, attrlength,
        varnamepattern=None):
    """create a function to add variable information to a dataset.
    
    dsname is the dataset name to append to.
    filetypes is the list of file types to include.
    filenamepattern is a regular expression to filter filename roots.
    dsvars is a special dictionary of variables and attributes.  See function addunique.
    attrindexes is a dictionary with keys of lower case attribute names and values as the dataset index starting with 0.
    attrlength is the size of the attribute string variables"""

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
    
    ll = len(dsvars)
    includeAttrs = ll > 3
    blanks = (ll-3) * [" "]

    def addinfo(filespec):
        """open the file if appropriate type, extract variable information, and add it to dataset dsname.
        
        filespec is the file to open
        dsname is the dataset name to append to
        filetypes is the list of file types to include."""
        
        fnsplit = os.path.split(filespec)[1]
        fn, ext = os.path.splitext(fnsplit)
        ext = ext.lower()
        for ft in filetypes:
            if ext in ftdict[ft]:
                if pat is None or pat.match(fn):
                    try:
                        spss.Submit(spsscmd[ft] % filespec)
                        spss.Submit("DATASET NAME @__GATHERMD__.")
                    except:
                        raise EnvironmentError(_("File could not be opened, skipping: %s") % filespec)
                    break
        else:
            return addinfo
        
        with DataStep():
            ds = spss.Dataset(name=dsname)  # not the active dataset
            dssource = spss.Dataset(name="*")  # The dataset to examine
            numvars = spss.GetVariableCount() # active dataset
            variables = dssource.varlist
            for v in range(numvars):
                lis = [filespec.replace("\\","/"), spss.GetVariableName(v), spss.GetVariableLabel(v)]
                if vpat is None or vpat.match(lis[1]):
                    lis.extend(blanks)
                    lis = [item+ 256*" " for item in lis]
                    ds.cases.append(lis)
                    if includeAttrs:
                        attrs = variables[v].attributes.data
                        for a in attrs:
                            if a.lower() in attrindexes:
                                ds.cases[-1, attrindexes[a.lower()]+ 3] = attrs[a][0] +  attrlength * " "# allow for standard variables
        spss.Submit("DATASET CLOSE @__GATHERMD__.")
    return addinfo

def addunique(dsdict, key):
    """Add modified version of key to dictionary dsdict.  Return generated key.
    
    dsdict is a dictionary whose keys will be lower case strings and whose values are unique SPSS variable names.
    duplicate keys are ignored.
    keys are automatically prefixed with "*" to separate them from variable names that could be identical."""
    
    key1 = "*" + key.lower()
    if key1 in dsdict:
        return key1

    # make a version of key that is unique in the dictionary values and a legal variable name length
    i=0
    keymod = spssaux.truncatestring(key, 64)
    while keymod.lower() in [k.lower() for k in list(dsdict.values())]:
        keymod = spssaux.truncatestring(key, 59) + "_" + str(i)
        i += 1
    dsdict[key1] = keymod
    return key1

escapelist = [('\a', r'\a'), ('\b', r'\b'), ('\f', r'\f'), ('\n', r'\n'), ('\r', r'\r'), ('\t',r'\t'),('\v', r'\v')]

def fixescapes(item):
    return(re.sub(r"\\", "/", item))
    #for esc, repl in escapelist:
        #item = item.replace(esc, repl)
    ###return item
    
   
# Example.
'''dsname = gather(["c:/temp/firstlevel"], filetypes=['spss','sas'], attrlist=['importance', 'relevance', 'VariableLabel'])

spss.Submit(r"""DATASET ACTIVATE %s.
SAVE OUTFILE='c:/temp2/gathered.sav'.""" % dsname)

dsname=gather(["c:/spss16/samples/employee data.sav"])'''