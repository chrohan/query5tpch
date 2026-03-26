#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <string>

using namespace std;

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    // TODO: Implement command line argument parsing
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
     
    for(int i = 0; i < argc; i++) {
        string arg = argv[i];
        if(arg == "--r_name"){
            r_name = argv[++i];   
        }else if(arg == "--start_date") {
            start_date = argv[++i];
        }else if(arg == "--end_date"){
            end_date = argv[++i];
        }else if(arg == "--threads"){
            num_threads = stoi(argv[++i]);
        }else if(arg == "--table_path") {
            table_path = argv[++i];
        }else if(arg == "--result_path") {
            result_path = argv[++i];
        }
    }
    
    if(r_name.empty() || start_date.empty() || end_date.empty() || num_threads <= 0 || table_path.empty() || result_path.empty()){
        return false;

    }
    return true;


}


bool readTable(const string& filename, const vector<int>& columns, const vector<string>& column_names, vector<map<string,string>>& table) {
    ifstream file(filename);
    if(file.is_open() == false){
        cout << "failed to open file" << endl;
        return false;
    }
    string line;
    while(getline(file, line)){
        if(line.size() == 0){
            continue;
        }
        vector<string> allCols;

        string curr = "";
        for(int i = 0; i < line.size(); i++){
             if(line[i] == '|'){
                allCols.push_back(curr);
                curr = "";
             }else{
                curr += line[i];
             }
        }
        
        map<string, string>row;

        for(int i = 0; i < columns.size(); i++){
            row[column_names[i]] = allCols[columns[i]];
        }

        table.push_back(row);
    }

    return true;

}
bool readTPCHData(const string& table_path, vector<map<string,string>>& customer_data, vector<map<string, string>>& orders_data, vector<map<string, string>>& lineitem_data, vector<map<string, string>>& supplier_data, vector<map<string, string>>& nation_data, vector<map<string, string>>& region_data) {
    //logic to read from path
    // ifstream file(table_path);
    // if(file.is_open() == false){
    //     cout << "failed to open file" << endl;
    //     return false;
    // }
   
    //go through the query and list all the columns that are needed only and ignore rest by |
    bool readCustomer = readTable(table_path + "customer.tbl", {0, 3}, {"c_custkey", "c_nationkey"}, customer_data);
    bool readOrders = readTable(table_path + "orders.tbl", {0, 1, 4}, {"o_orderkey", "o_custkey", "o_orderdate"}, orders_data);
    bool readLineitem = readTable(table_path + "lineitem.tbl",{0, 2, 5, 6},{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"}, lineitem_data);
    bool readSupplier = readTable(table_path + "supplier.tbl", {0, 3},{"s_suppkey", "s_nationkey"},supplier_data);
    bool readNation = readTable(table_path + "nation.tbl", {0, 1, 2},{"n_nationkey", "n_name", "n_regionkey"}, nation_data);
    bool readRegion = readTable(table_path + "region.tbl",{0, 1}, {"r_regionkey", "r_name"}, region_data);

    return readCustomer && readOrders && readLineitem && readSupplier && readNation && readRegion;

   
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads,
     const std::vector<std::map<std::string, std::string>>& customer_data, 
     const std::vector<std::map<std::string, std::string>>& orders_data, 
     const std::vector<std::map<std::string, std::string>>& lineitem_data, 
     const std::vector<std::map<std::string, std::string>>& supplier_data, 
     const std::vector<std::map<std::string, std::string>>& nation_data, 
     const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
// where 
// c_custkey = o_custkey 
// and l_orderkey = o_orderkey 
// and l_suppkey = s_suppkey 
// and c_nationkey = s_nationkey 
// and s_nationkey = n_nationkey                     3. suppliers,   4. customer  both same nation,  6. use supplier to get l 5. get order, custo, revenue
// and n_regionkey = r_regionkey                     2. find nation
// and r_name = 'ASIA'                               1. find region
// and o_orderdate >= '1994-01-01' 
// and o_orderdate < '1995-01-01' 

    //getting required region
     string regionkey = "";

     for(auto& region : region_data){
        // if(region.find("r_name") != region.end() && region["r_name"] == r_name){
        //     regionkey = region["r_regionkey"];
        if(region.at("r_name") == r_name){
            regionkey = region.at("r_regionkey");
            break;
        }
     }

     if(regionkey.size() == 0){
        cout << "Region not found" << endl;
        return false;
     }

     //getting all the nations of the found region
     map<string,string> nationkey_to_name;

     for(auto& nation : nation_data){
        if(nation.at("n_regionkey") == regionkey){
            string key = nation.at("n_nationkey");
            string value = nation.at("n_name");
            nationkey_to_name[key] = value;
        }
     }


     //getting all the suppliers of those nation
     map<string,string> suppkey_to_nationkey;
     for(auto& suppliers : supplier_data){
        string s_nation = suppliers.at("s_nationkey");
        if(nationkey_to_name.find(s_nation) != nationkey_to_name.end()){
            suppkey_to_nationkey[suppliers.at("s_suppkey")] = s_nation;
        }
     }

     //getting all the customers of those nations
     map<string,string>custkey_to_nationkey;
     for(auto& customers : customer_data){
        string c_nation = customers.at("c_nationkey");
        if(nationkey_to_name.find(c_nation) != nationkey_to_name.end()){
            custkey_to_nationkey[customers.at("c_custkey")] = c_nation;
        }
     }


     //getting orders from those nations
     map<string,string>orderkey_to_nationkey;
     for(auto& orders : orders_data){
        string ckey = orders.at("o_custkey");
        if(custkey_to_nationkey.find(ckey) == custkey_to_nationkey.end())continue;
        if(orders.at("o_orderdate") < start_date || orders.at("o_orderdate") >= end_date)continue;

        string nation = custkey_to_nationkey[ckey];
        orderkey_to_nationkey[orders.at("o_orderkey")] = nation;

     }

     //logic for revenue calculn
     for(auto& lineitems : lineitem_data){
        string orderkey = lineitems.at("l_orderkey");
        string suppkey = lineitems.at("l_suppkey");
         
        if(orderkey_to_nationkey.find(orderkey) == orderkey_to_nationkey.end())continue;
        if(suppkey_to_nationkey.find(suppkey) == suppkey_to_nationkey.end())continue;
        if(orderkey_to_nationkey[orderkey] != suppkey_to_nationkey[suppkey])continue;

        // (l_extendedprice * (1 - l_discount)) as revenue 

        double extendedprice = stod(lineitems.at("l_extendedprice"));
        double discount = stod(lineitems.at("l_discount"));

        double revenue = extendedprice * (1 - discount);

        results[nationkey_to_name[orderkey_to_nationkey[orderkey]]] += revenue;

     }
return true;
    
}

  bool cmp(pair<string,double>& a, pair<string,double>& b){
    return a.second > b.second;
  }

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    //desc!!!!!!!!!!

    vector< pair<string,double>> sorted_result;

    for(auto& it : results){
        sorted_result.push_back({it.first, it.second});
    }

    sort(sorted_result.begin() , sorted_result.end(), cmp);

    ofstream file(result_path + "output.txt");
    if(file.is_open() == false){
        cout << "Failed to open output file" << endl;
        return false;
    }

    file << "n_name" << "|" << "revenue" << "\n";
    for(auto& entry : sorted_result){
        file << entry.first << "|" << entry.second << "\n";
    }

    return true;

} 