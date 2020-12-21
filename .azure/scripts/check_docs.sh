#!/usr/bin/env bash

fatal=0
GREP=grep

if [ $(uname -s) = "Darwin" ]; then
  GREP=ggrep
fi

function grep_check {
  local pattern=$1
  local description=$2
  local opts=${3:--i -E -r -n}
  local fatalness=${4:-1}
  local excludes="--exclude-dir=logo --exclude-dir=images --exclude-dir=contributing --exclude-dir=html --exclude-dir=htmlnoheader --exclude-dir=pdf"
  x=$($GREP $opts $excludes "$pattern" documentation/)
  if [ -n "$x" ]; then
    echo "$description:"
    echo "$x"
    y=$(echo "$x" | wc -l)
    ((fatal+=fatalness*y))
  fi
}

# Check for latin abbrevs
grep_check '[^[:alpha:]](e\.g\.|eg)[^[:alpha:]]' "Replace 'e.g'. with 'for example, '"
grep_check '[^[:alpha:]](i\.e\.|ie)[^[:alpha:]]' "Replace 'i.e'. with 'that is, '"
grep_check '[^[:alpha:]]etc\.[^[:alpha:]]?' "Replace 'etc.'. with ' and so on.'"

# And/or
grep_check '[^[:alpha:]]and/or[^[:alpha:]]' "Use either 'and' or 'or', but not 'and/or'"

# Contractions
grep_check '[^[:alpha:]](do|is|are|won|have|ca|does|did|had|has|must)n'"'"'?t[^[:alpha:]]' "Avoid 'nt contraction"
grep_check '[^[:alpha:]]it'"'"'s[^[:alpha:]]' "Avoid it's contraction"
grep_check '[^[:alpha:]]that'"'"'s[^[:alpha:]]' "Avoid that's contraction"
grep_check '[^[:alpha:]]can not[^[:alpha:]]' "Use 'cannot' not 'can not'"

# Asciidoc standards
grep_check '[<][<][[:alnum:]_-]+,' "Internal links should be xref:doc_id[Section title], not <<doc_id,link text>>"
grep_check '[[]id=(["'"'"'])[[:alnum:]_-]+(?!-[{]context[}])\1' "[id=...] should end with -{context}" "-i -P -r -n"

# leveloffset=+
grep_check 'leveloffset+=[0-9]+'  "It should be 'leveloffset=+...' not '+='"

# include: should be include::
grep_check 'include:[^:[ ]+[[]'  "It should be 'include::...[]' (two colons) not 'include:...[]'"

if [ $fatal -gt 0 ]; then
  echo "ERROR: ${fatal} docs problems found."
  exit 1
fi

# Check for changes in autogenerated code
make docu_versions
CHANGED_DERIVED=$(git diff --name-status -- documentation/book/)
if [ -n "$CHANGED_DERIVED" ] ; then
  echo "ERROR: Uncommitted changes in documentation:"
  echo "$CHANGED_DERIVED"
  echo "Run the following to add up-to-date resources:"
  echo "  make docu_versions \\"
  echo "    && git add documentation/ \\"
  echo "    && git commit -s -m 'Update generated documentation'"
  exit 1
fi