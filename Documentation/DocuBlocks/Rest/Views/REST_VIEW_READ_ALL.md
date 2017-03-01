
@startDocuBlock REST_VIEW_READ_ALL
@brief returns all views

@RESTHEADER{GET /_api/view,Read all views}

@RESTURLPARAMETERS

@RESTHEADERPARAMETERS

@RESTDESCRIPTION
Returns an object with an attribute *views* containing an
array of all view descriptions. The same information is also
available in the *names* as an object with the view names
as keys.

@RESTRETURNCODES

@RESTRETURNCODE{200}
The list of views
@endDocuBlock
