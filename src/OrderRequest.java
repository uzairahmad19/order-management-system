class OrderRequest {
    int symbolId;
    double price;
    long qty;
    char side; // B or S
    long orderId;
    RequestType type;
}