## Notes: ##

* The roles that are null are allocated to 'unknown' role
* There were also 12 users with roles [“admin”,”analyst”], [“admin”,”driver”] and those have been allocated 'admin' as role 
*	It was observed that in the dimension table, ‘user_roles’, there are repeated user IDs i.e. the same user has 2 roles
*	It is not best practice to have duplicate primary keys in a dim table unless it is intentional
*	The first thought of using a composite key, but the ‘roles’ column is not available in the ‘requests_log’ table
*	The second thought was to remove duplicates, but then there might be information loss, as a user can have 2 roles at a time
*	It was observed that in the same table, there are repeated user IDs with blank roles, i.e. the same user has 1 role as admin, and the other role is blank. Again, to avoid loss of information, I did not remove those data points
  






## Result: ##

<img width="446" alt="image" src="https://github.com/sanski96yadav/finn/assets/175153827/ed649d79-5735-4522-ac61-090409faa1a0">

