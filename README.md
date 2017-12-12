# BonitaProperties
In 7.3, BonitaHome does not exist. So, this library save the Properties in the BonitaDatabase itself. It's working in a Cluster too

1.0 First version
1.1 Add the DomainName concept.
The new method are available to load only for one domain name, and table change. The update is automaticaly done by the librairy

1.2 Add checkDatabase possibility : the database is not chack at each connection to improve performance
1.3 Add administration method, REST API to get / put / admin access

1.4 - Manage large value in the key (value size more than the size of the column). 
		- Check the autocommit and not commit in case of autocommit. 
		- Add the tenantId in the database
1.4.1 Fix bug in Oracle
1.4.2 Fix bug on connection managment


