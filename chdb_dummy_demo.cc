#include "chdb/src/tx_region.h"

int main() {
    chdb store(30, CHDB_PORT);
    int r = 0;
    for (int i = 0; i < 4; ++i) {
        tx_region db_client(&store);
        // dummy request
        int r = db_client.dummy();
        db_client.put(1, 1024);
        r = db_client.get(1);
        printf("Get first\tresp:%d\n", r);
    }
}