
@startDocuBlock REST_VIEW_DELETE
@brief drops a view or link

@RESTHEADER{DELETE /_api/view/{view-name}, Drop a view or a collection from a view}

@RESTQUERYPARAM{collection,string,optional}
The collection name.

@RESTURLPARAMETERS

@RESTURLPARAM{view-name,string,required}
The name of the view to drop.

@RESTQUERYPARAMETERS

@RESTDESCRIPTION
Drops the view identified by *view-name* or the view link to the
collection identified by *collection* 

If the view was successfully dropped, an object is returned with
the following attributes:

- *error*: *false*

- *id*: The identifier of the dropped view 

@RESTRETURNCODES

@RESTRETURNCODE{400}
If the *view-name* is missing, then a *HTTP 400* is
returned.

@RESTRETURNCODE{404}
If the *view-name* is unknown, then a *HTTP 404* is returned.
If the *collecton* is unknown, then a *HTTP 404* is returned.
If the *view-name* does not contain link to the *collection*, 
then a *HTTP 404* is returned.

@endDocuBlock


