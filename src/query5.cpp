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
        if(argv[i] == "--r_name"){
            r_name = argv[i+1];   
        }else if(argv[i] == "--start_date") {
            start_date = argv[i+1];
        }else if(argv[i] == "--end_date"){
            end_date = argv[i+1];
        }else if(argv[i] == "--threads"){
            num_threads = stoi(argv[i+1]);
        }else if(argv[i] == "--table_path") {
            table_path = argv[i+1];
        }else if(argv[i] == "--result_path") {
            result_path = argv[i+1];
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
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    // TODO: Implement TPCH Query 5 using multithreading
    return false;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    // TODO: Implement outputting results to a file
    return false;
} 