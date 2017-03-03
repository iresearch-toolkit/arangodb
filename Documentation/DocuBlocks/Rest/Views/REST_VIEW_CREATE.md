
@startDocuBlock REST_VIEW_CREATE
@brief creates a view

@RESTHEADER{POST /_api/view, Create a view}

@RESTBODYPARAM{name,string,required,string}
The name of the view.

@RESTBODYPARAM{type,string,required,string}
The type of the view to create.
The following values for *type* are valid:

- *iresearch*: iresearch view

@RESTDESCRIPTION
Creates a new view with a given name. The request must contain an
object with the following attributes.

The type of the view to be created must specified in the *type*
attribute of the view details. Depending on the view type, additional
other attributes may need to specified in the request in order to create
the index.

@RESTRETURNCODES

@RESTRETURNCODE{200}
If the view already exists, then an *HTTP 200* is returned.

@RESTRETURNCODE{201}
If the view does not already exist and could be created, then an *HTTP 201*
is returned.

@RESTRETURNCODE{400}
If the *view-name* is missing, then a *HTTP 400* is
returned.

@endDocuBlock

