# cardano_decentralization
Monitor the decentralization level of Cardano blockchain.

## Cardano Decentralization API
### Prerequisites
1. Build and run Cardano on you Linux system, following this document: https://cardano-community.github.io/guild-operators/build  .
2. Make sure that you are accessible for cexplorer database.
3. Build views by following this document: https://github.com/cardanocanuck/db-sync-queries  .
4. Install python3, and install librarys: pandas/json/psycopg2/flask/math  .
5. Clone all files to your computer, and run <python3 api_launcher.py> under right dir in terminal

### How to get data 
The api address is : http://"your ip address":5000/api_collection/<api name>. For example, suppose my ip address is 69.101.34.1, and I want to run gini_api.py to see the gini coefficient of Cardano， then I input: http://69.101.34.1:5000/api_collection/gini  in my browser.

You can see all APIs in the api/api_collection directory.

 
## Cardano Decentralization Website
This website is developed based on Node.js. This website is just a visualization tool. 
Because the website relays on the data in my server, please do visit it from : https://www.jialiang.info


