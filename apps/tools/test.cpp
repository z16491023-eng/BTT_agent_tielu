#include <fstream>
#include <string>
#include <iostream>
#include <ctime>
using namespace std;


class strFilter {
 

  public:
    string Reverse(const string& input) {
        string result;
        for (int i=input.length()-1;i>=0;i--){
            result +=input[i];
        }
        return result;
    }
};

int main() {
    time_t now = time(NULL);
    tm *ltm = localtime(&now);
    cout << "now: " << now << endl;
    cout << "local time: " << asctime(ltm) << endl;
    return 0;
}
