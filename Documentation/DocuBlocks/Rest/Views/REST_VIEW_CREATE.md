
@startDocuBlock REST_VIEW_CREATE
@brief creates a view

@RESTHEADER{POST /_api/view, Create view}

@RESTBODYPARAM{name,string,required,string}
The name of the view.

@RESTDESCRIPTION
Creates a new view with a given name. The request must contain an
object with the following attributes.

@RESTRETURNCODES

@RESTRETURNCODE{400}
If the *view-name* is missing, then a *HTTP 400* is
returned.

@RESTRETURNCODE{404}
If the *view-name* is unknown, then a *HTTP 404* is returned.

@endDocuBlock

