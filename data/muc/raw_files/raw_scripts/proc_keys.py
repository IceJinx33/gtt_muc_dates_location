"""
Process the crazy-ass MUC keyfile format into a reasonable JSON thing
    cat data/TASK/CORPORA/dev/key-dev-0* | python proc_keys.py

for unit tests (http://pytest.org/)
    py.test proc_keys.py
"""
import sys, re
import json
from pprint import pprint


def cleankey(keystr):
    return re.sub(r"[^A-Z]+", "_", keystr).strip("_").lower()


def clean_docid(value):
    return re.sub(r"\s*\(.*$", "", value)


ALL_KEYS = """
MESSAGE: ID
MESSAGE: TEMPLATE
INCIDENT: DATE
INCIDENT: LOCATION
INCIDENT: TYPE
INCIDENT: STAGE OF EXECUTION
INCIDENT: INSTRUMENT ID
INCIDENT: INSTRUMENT TYPE
PERP: INCIDENT CATEGORY
PERP: INDIVIDUAL ID
PERP: ORGANIZATION ID
PERP: ORGANIZATION CONFIDENCE
PHYS TGT: ID
PHYS TGT: TYPE
PHYS TGT: NUMBER
PHYS TGT: FOREIGN NATION
PHYS TGT: EFFECT OF INCIDENT
PHYS TGT: TOTAL NUMBER
HUM TGT: NAME
HUM TGT: DESCRIPTION
HUM TGT: TYPE
HUM TGT: NUMBER
HUM TGT: FOREIGN NATION
HUM TGT: EFFECT OF INCIDENT
HUM TGT: TOTAL NUMBER
""".strip().split(
    "\n"
)

ALL_KEYS = set(cleankey(k) for k in ALL_KEYS)

KEY_WHITELIST = """
perp_individual_id
perp_organization_id
phys_tgt_id
hum_tgt_name
hum_tgt_description
incident_instrument_id
incident_date
incident_location
""".split()

KEY_WHITELIST = set(KEY_WHITELIST)

assert KEY_WHITELIST <= ALL_KEYS

cur_docid = None


def warning(s):
    global cur_docid
    print>>sys.stderr, "WARNING docid=%s | %s" % (cur_docid, s)


def yield_keyvals(chunk):
    """
    Processes the raw MUC "key file" format.  Parses one entry ("chunk").
    Yields a sequence of (key,value) pairs.
    A single key can be repeated many times.
    This function cleans up key names, but passes the values through as-is.
    """
    curkey = None
    for line in chunk.split("\n"):
        if line.startswith(";"):
            yield "comment", line
            continue
        middle = 33  ## Different in dev vs test files... this is the minimum size to get all keys.
        keytext = line[:middle].strip()
        valtext = line[middle:].strip()
        if not keytext:
            ## it's a continuation
            assert curkey
        else:
            curkey = cleankey(keytext)
            assert curkey in ALL_KEYS

        yield curkey, valtext


def parse_values(keyvals):
    """
    Takes key,value pairs as input, where the values are unparsed.
    Filter down to the slots we want, and parse their values as well.
    """
    for key, value in keyvals:
        if key == "message_id":
            yield key, clean_docid(value)
            continue
        if key == "message_template":
            if re.search(r"^\d+$", value):
                yield key, int(value)
            elif value == "*":
                yield key, value
            elif re.search(r"^\d+ \(OPTIONAL\)$", value):
                yield key, int(value.split()[0])
                yield "message_template_optional", True
            else:
                assert False, "bad message_template format"
            continue
        if key == "incident_type":
            yield key, clean_docid(value)
            continue

        if key in KEY_WHITELIST:
            if value == "*":
                continue

            if value == "-":
                yield key, None
                continue

            if '"' not in value:
                warning(
                    "apparent data error, missing quotes. adding back in. value was ||| %s"
                    % value
                )
                value = '"' + value + '"'

            if key == "incident_location":
                value = parse_loc_value(value)
            elif key == "incident_date":
                value = parse_date_value(value)
            else:
                value = parse_one_value(value)
            yield key, value


def parse_loc_value(namestr):
    """
    Returns a dictionary with 'type' either
        'simple_strings' ==> has a field 'strings'
        'colon_clause'   ==> has multiple location fields string_loc_x'
    """
    
    #namestr = namestr.replace("? ", "")
    
    d = {}

    expr = r':[^\(\)]+\([^\(\)]+/[^\(\)]+\)'
    reg = re.search(expr, namestr)

    if reg != None:
        elt_string = re.sub(r'[:\)\/\"]', '', namestr[reg.start():reg.end()+1])
        ind_rep = elt_string.index('(')
        entity = elt_string[:ind_rep].strip()
        tags = elt_string[ind_rep+1:].split()
        namestr = namestr[:reg.start()] + ''.join([': ' + entity + ' (' + t + ') ' for t in tags]) + namestr[reg.end():]

    alt_strings = re.split(" */ *", namestr)
    alt_strings = [ss.strip() for ss in alt_strings]
    j = 0
    for a in alt_strings:
        anew = ('"' if a[0] != '"' else '') + a + ('"' if a[-1] != '"' else '')
        
        if anew[1] == "(" and anew[-2] == ")":
            anew = anew[0] + anew[2:-2] + anew[-1]

        anew = anew.replace("GUATAMALA", "GUATEMALA")

        if ":" in anew:
            loc_lst = anew.split(":")
            new_loc_lst = []
            for ln in loc_lst:
                new_loc_lst += ln.strip().split(' - ')
            
            for i, l in enumerate(new_loc_lst):

                lnew = ('"' if l[0] != '"' else '') + l + ('"' if l[-1] != '"' else '')

                if i == 0:
                    loc_value = parse_strings_possibly_with_alternations(lnew)
                elif i == len(loc_lst) - 1:
                    loc_value = parse_strings_possibly_with_alternations(lnew)
                else:
                    loc_value = parse_strings_possibly_with_alternations(lnew)

                if loc_value[0] == "SAN SALVADOR":
                    loc_value = ["EL SALVADOR"]

                found = False
                found2 = False
                words = loc_value[0].split()
                for k in d:
                    if loc_value[0] in d[k]:
                        found = True
                    else:
                        loc_words = " " + " ".join(d[k]) + " "
                        for w in words:
                            if ((loc_value[0] == "SAN SALVADOR (CITY)" and "SAN SALVADOR (DEPARTMENT)" in loc_words)
                               or (loc_value[0] == "SAN SALVADOR (DEPARTMENT)" and "SAN SALVADOR (CITY)" in loc_words)):
                                found2 = True
                                found_key = k
                            if (not(loc_value[0] == "GUATEMALA CITY (CITY)" or 
                                   loc_value[0] == "SAN SALVADOR (CITY)" or 
                                   loc_value[0] == "EL CARMEN (DISTRICT)" 
                                   or loc_value[0] == "EL TRIUNFO (TOWN)"
                                   or loc_value[0] == "SAN FRANCISCO (NEIGHBORHOOD)"
                                   or loc_value[0] == "SAN ANDRES (HAMLET)"
                                   or loc_value[0] == "SAN LUIS DE LA REINA (TOWN)"
                                   or loc_value[0] == "SAN JOSE (NEIGHBORHOOD)"
                                   or loc_value[0] == "SAN SALVADOR (DEPARTMENT)"
                                   or loc_value[0] == "BOGOTA AIRPORT (AIRPORT)"
                                   or loc_value[0] == "SAN MIGUELITO (NEIGHBORHOOD)"
                                   or loc_value[0] == "SAN ANTONIO ABAD (NEIGHBORHOOD)"
                                   or loc_value[0] == "EL CONGO (CITY)"
                                   or loc_value[0] == "EL SALVADOR UNIVERSITY (UNIVERSITY)"
                                   or loc_value[0] == "SAN BENITO (NEIGHBORHOOD)"
                                   or loc_value[0] == "MEDELLIN AIRPORT (AIRPORT)"
                                   or loc_value[0] == "UNIVERSITY OF EL SALVADOR (UNIVERSITY)"
                                   or loc_value[0] == "SAN MIGUEL (CITY)"
                                   or loc_value[0] == "GUATEMALA CITY (CAPITAL)"
                                   or loc_value[0] == "PANAMA CITY (CITY)"
                                   or loc_value[0] == "GUATEMALA (DEPARTMENT)"
                                   or loc_value[0] == "EL CENTENARIO (TOWN)"
                                   or loc_value[0] == "SAN PABLOS (CITY)"
                                   or loc_value[0] == "SAN PABLO (TOWN)"
                                   or w == "(CITY)" or w == "(TOWN)")
                                   and " "+w+" " in loc_words):
                                found2 = True
                                found_key = k
                if found:
                    pass
                else:
                    if found2:
                        d[found_key].append(loc_value[0])
                    elif "string_loc_" + str(i + 1) not in d:
                        d.update({"string_loc_" + str(i + 1): loc_value})
                        j = i+1
                    else:
                        d["string_loc_" + str(j + 1)] = loc_value
                        j += 1

        else:
            new_loc_lst = anew.split(' - ')
            for l in new_loc_lst:
                strings = parse_strings_possibly_with_alternations(('"' if l[0] != '"' else '') + l + ('"' if l[-1] != '"' else ''))
                found3 = False
                for k in d:
                    if strings[0] in d[k]:
                        found3 = True
                        break
                if not found3:
                    d.update({"string_loc_" + str(j + 1): strings})
                    j += 1
    return d

def parse_date_value(namestr):
    
    d = {}
    namestr = re.sub(r'[\(\)]', '', namestr)

    new_lst = namestr.split(' / ')
    if len(new_lst) > 1:
        d["strings"] = [parse_strings_possibly_with_alternations(('"' if elt[0] != '"' else '') + elt + ('"' if elt[-1] != '"' else ''))[0] for elt in new_lst]
    else:
        d["strings"] = parse_strings_possibly_with_alternations(('"' if namestr[0] != '"' else '') + namestr + ('"' if namestr[-1] != '"' else ''))
    return d

def parse_one_value(namestr):
    """
    Returns a dictionary with 'type' either
        'simple_strings' ==> has a field 'strings'
        'colon_clause'   ==> has two fields 'strings_lhs' and 'strings_rhs'
    Furthermore, has 'optional':true  if this valueline is optional, which I think means the entity is optional.
    (There is only one example of a colon clause having optional=true; I suspect it's an annotation error.)
    """
    warning(namestr)

    global cur_docid
    # Fix bugs in the data
    if cur_docid == "DEV-MUC3-0604" and "BODYGUARD OF EL ESPECTADOR" in namestr:
        # DEV-MUC3-0604 (MDESC)
        # ? ("BODYGUARD OF EL ESPECTADOR'S CHIEF OF DISTRIBUTION IN MEDELLIN" / "BODYGUARD"): "PEDRO LUIS OSORIO"
        namestr = '''? "BODYGUARD OF EL ESPECTADOR'S CHIEF OF DISTRIBUTION IN MEDELLIN" / "BODYGUARD" / "PEDRO LUIS OSORIO"'''
    if namestr == 'MACHINEGUNS"':
        # DEV-MUC3-0217
        namestr = '"' + namestr

    d = {}
    match = re.search(r"\? *(.*)", namestr)
    if match:
        d["optional"] = True
        namestr = match.group(1)

    if ":" in namestr:
        assert len(re.findall(":", namestr)) == 1
        lhs, rhs = re.split(r" *: *", namestr)
        lhs_value = parse_strings_possibly_with_alternations(lhs)
        rhs_value = parse_strings_possibly_with_alternations(rhs)
        d.update(
            {"type": "colon_clause", "strings_lhs": lhs_value, "strings_rhs": rhs_value}
        )
        return d

    else:
        strings = parse_strings_possibly_with_alternations(namestr)
        d.update({"type": "simple_strings", "strings": strings})
        return d


def parse_strings_possibly_with_alternations(namestr):
    namestr = namestr.strip()
    assert ":" not in namestr, namestr
    assert not namestr.startswith("?")
    parts = re.split(" */ *", namestr)
    parts = [ss.strip() for ss in parts]
    strings = []
    for ss in parts:
        if ss == "-":
            # We should see this only inside a colon clause. There are a few of these, e.g.
            # 21. HUM TGT: NUMBER                 -: "ORLANDO LETELIER"
            strings.append(None)
            continue
        if not (ss[0] == '"' and ss[-1] == '"'):
            warning("WTF ||| " + ss)
        ss = ss[1:-1]
        ss = ss.decode("string_escape")  # They seem to use C-style backslash escaping
        ss = ss.strip()
        strings.append(ss)
    return strings


def test_parsestrings():
    f = parse_strings_possibly_with_alternations
    s = '"CAR DEALERSHIP"'
    assert set(f(s)) == {"CAR DEALERSHIP"}
    s = '"TUPAC AMARU REVOLUTIONARY MOVEMENT" / "MRTA"'
    assert set(f(s)) == {"TUPAC AMARU REVOLUTIONARY MOVEMENT", "MRTA"}


def test_parse_one_value():
    s = '"U.S. JOURNALIST": "BERNARDETTE PARDO"'
    d = parse_one_value(s)
    assert d["strings_lhs"] == ["U.S. JOURNALIST"]
    assert d["strings_rhs"] == ["BERNARDETTE PARDO"]


def test_parse_loc_value():
    s1 = "EL SALVADOR: SAN MIGUEL (DEPARTMENT): SAN LUIS DE LA REINA (TOWN)"
    s2 = "(EL SALVADOR: MORAZAN (DEPARTMENT): YAMABAL (CANTON)) / (EL SALVADOR: MORAZAN (COUNTY): YAMABAL (CANTON))"
    s3 = "COLOMBIA: URABA (REGION / CITY)"
    s4 = "(COLOMBIA: NORTE DE SANTANDER (DEPARTMENT): URUM (MUNICIPALITY)) / (COLOMBIA: NORTE DE SANTANDER (DEPARTMENT): URUM (MUNICIPALITY): CANO LIMON (CITY) - COVENAS (CITY))"
    s5 = "CANO LIMON (CITY) - COVENAS (CITY)"
    s6 = "EL SALVADOR: USULUTAN (CITY / COUNTY / DEPARTMENT)"
    d1 = parse_loc_value(s1)
    d2 = parse_loc_value(s2)
    d3 = parse_loc_value(s3)
    d4 = parse_loc_value(s4)
    d5 = parse_loc_value(s5)
    print(d1)
    print(d2)
    print(d3)
    print(d4)
    print(d5)
    assert d1["string_loc_1"] == ["EL SALVADOR"]
    assert d1["string_loc_2"] == ["SAN MIGUEL (DEPARTMENT)"]
    assert d1["string_loc_3"] == ["SAN LUIS DE LA REINA (TOWN)"]
    assert d2["string_loc_1"] == ["EL SALVADOR"]
    assert d2["string_loc_2"] == ["MORAZAN (DEPARTMENT)", "MORAZAN (COUNTY)"]
    assert d2["string_loc_3"] == ["YAMABAL (CANTON)"]
    assert d3["string_loc_1"] == ["COLOMBIA"]
    assert d3["string_loc_2"] == ["URABA (REGION)","URABA (CITY)"]
    assert d4["string_loc_1"] == ["COLOMBIA"]
    assert d4["string_loc_2"] == ["NORTE DE SANTANDER (DEPARTMENT)"]
    assert d4["string_loc_3"] == ["URUM (MUNICIPALITY)"]
    assert d4["string_loc_4"] == ["CANO LIMON (CITY)"]
    assert d4["string_loc_5"] == ["COVENAS (CITY)"]
    assert d4["string_loc_1"] == ["CANO LIMON (CITY)"]
    assert d4["string_loc_2"] == ["COVENAS (CITY)"]


def fancy_json_print(keyvals):
    lines = [json.dumps(kv, sort_keys=True) for kv in keyvals]
    s = ""
    s += "[\n  "
    s += ",\n  ".join(lines)
    s += "\n]"
    return s


if __name__ == "__main__":

    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--format", default="jsonpp", choices=["jsonpp", "sidebyside"])
    args = p.parse_args()

    if args.format == "sidebyside":
        print(
            """
            <style> pre { white-space: pre-wrap; } </style>
        """
        )
        print("<table cellpadding=3 border=1 cellspacing=0 width='100%'>")

    data = sys.stdin.read()
    lines = data.split("\n")
    lines = [L for L in lines if not re.search(r"^\s*;", L)]  ## comments
    data = "\n".join(lines)
    chunks = re.split(r"\n\n+|\n(?=0\. )", data)
    chunks = [c.strip() for c in chunks if c.strip()]

    for chunk in chunks:
        # print "==="; print chunk

        keyvals1 = list(yield_keyvals(chunk))
        assert all(k in ALL_KEYS or k == "comment" for k, v in keyvals1)
        cur_docid = clean_docid(dict(keyvals1)["message_id"])
        # print "===", cur_docid
        # print "--- raw key/val pairs"; pprint(keyvals1); print

        keyvals2 = list(parse_values(keyvals1))
        # print "--- parsed values"; pprint(keyvals2); print

        if args.format == "jsonpp":
            print("%%%")
            print(fancy_json_print(keyvals2))

        elif args.format == "sidebyside":
            print(
                "<tr><td><pre>{chunk}</pre> <td><pre>{json}</pre>".format(
                    chunk=chunk, json=fancy_json_print(keyvals2)
                )
            )

        else:
            assert False
