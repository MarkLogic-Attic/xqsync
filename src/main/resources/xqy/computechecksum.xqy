
declare variable $URI external;

(: must return 0 for empty or non-existence URIs :)

if ($URI and fn:doc-available($URI)) then
xdmp:md5(fn:concat(
  xdmp:quote(fn:doc($URI)),
  xdmp:quote(xdmp:document-properties($URI)),
  for $i in xdmp:quote(xdmp:document-get-collections($URI))
  order by $i
  return $i
))
else 0
