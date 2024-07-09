## Notes: ##

*	There are many companies that have different fleet sizes, so I assumed here that a company can have more than one fleet for various needs like departments, fuel type, different cities in which office is located, vehicle type, leasing or subscription type. For e.g. company ```10004458251``` has company_fleet_size as 70, 90, indicating the company has 2 fleets
*	There are also companies with 2 users with the same fleet size, e.g. 2 users from the same company have a ‘company_fleet_size’ of 50. In such cases, it has been assumed that those are 2 different fleets
*	There are 16 companies with zero as ‘company_fleet_size’
*	Companies are categorised into different sizes small, medium and large based on no. of cars in the fleet
* The data is partitioned by company, and then the sum of ‘company_fleet_size’ is done, as each ```company_fleet_size``` indicates no. of cars in that fleet
*	Based on the sum, the companies are divided into different sizes








## Result: ##

<img width="637" alt="image" src="https://github.com/sanski96yadav/finn/assets/175153827/0479f40f-53c7-4382-a175-60cd663149ed">
