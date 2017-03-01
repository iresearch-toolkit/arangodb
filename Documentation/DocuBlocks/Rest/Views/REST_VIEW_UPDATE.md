
@startDocuBlock REST_VIEW_UPDATE
@brief Updates a view

@RESTHEADER{PATCH /_api/view/{view-name}, Updates a view}

@RESTBODYPARAM{name,string,required,string}
The name of the view.

@RESTDESCRIPTION
Updates a view with a given name. The request must contain an
object with the following attributes.

@RESTRETURNCODES

@RESTRETURNCODE{400}
If the *view-name* is missing, then a *HTTP 400* is
returned.

@RESTRETURNCODE{404}
If the *view-name* is unknown, then a *HTTP 404* is returned.

@endDocuBlock
