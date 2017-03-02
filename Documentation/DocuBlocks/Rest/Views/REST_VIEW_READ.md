
@startDocuBlock REST_VIEW_READ
@brief reads a single view or view link

@RESTHEADER{GET /_api/view/{view-name},Read view or view link}

@RESTQUERYPARAM{collection,string,optional}
The collection name.

@RESTURLPARAMETERS

@RESTURLPARAM{view-name,string,required}
The name of the view.

@RESTHEADERPARAMETERS

@RESTDESCRIPTION
Returns the view identified by *view-name*.

@RESTRETURNCODES

@RESTRETURNCODE{200}
is returned if the view or link was found

@RESTRETURNCODE{404}
is returned if the view or link was not found

@endDocuBlock
