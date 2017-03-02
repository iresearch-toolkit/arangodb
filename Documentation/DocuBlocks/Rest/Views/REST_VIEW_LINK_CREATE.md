
@startDocuBlock REST_VIEW_LINK_CREATE
@brief adds collection to a view

@RESTHEADER{POST /_api/view/{view-name}, Add a collection to a view}

@RESTURLPARAMETERS

@RESTURLPARAM{view-name,string,required}
The name of the view.

@RESTBODYPARAM{collection,string,required,string}
The name of the view.

@RESTDESCRIPTION
Adds collection with a given name to a view with a given name.
The request must contain an object with the following attributes.

@RESTRETURNCODES

@RESTRETURNCODE{400}
If the *view-name* is missing, then a *HTTP 400* is
returned.

@RESTRETURNCODE{404}
If the *view-name* is unknown, then a *HTTP 404* is returned.
If the *collecton* is unknown, then a *HTTP 404* is returned.

@endDocuBlock

