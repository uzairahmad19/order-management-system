import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
public class OrderManagement {
    // These are the trading hours
    private final LocalTime marketOpen = LocalTime.of(10, 0); // 10 am
    private final LocalTime marketClose = LocalTime.of(13, 0); // 1 pm
    private volatile boolean isTradingTime = false; //market status

    private final int maxOrdersPerSecond = 100; //rate limit
    private final AtomicInteger ordersThisSecond = new AtomicInteger(0); //order counter
    private final Queue<OrderRequest> orderQueue = new ConcurrentLinkedQueue<>(); //pending orders

    private final Map<Long, Long> sentOrderTimes = new ConcurrentHashMap<>();// order id : sent time (map)

    // task scheduler
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public OrderManagement() {
        // This checks market status every second
        scheduler.scheduleAtFixedRate(this::checkMarketHours, 0, 1, TimeUnit.SECONDS);

        // This processes queued orders every second
        scheduler.scheduleAtFixedRate(this::processQueue, 0, 1, TimeUnit.SECONDS);
    }

    // Checks if current time is within trading hours
    // Updates market status and sends logon/logout messages
    private void checkMarketHours() {
        LocalTime now = LocalTime.now();
        boolean shouldBeOpen = !now.isBefore(marketOpen) && now.isBefore(marketClose);

        if (shouldBeOpen && !isTradingTime) { //trading time started
            isTradingTime = true;
            sendLogon();
        } else if (!shouldBeOpen && isTradingTime) { // trading time finished
            isTradingTime = false;
            sendLogout();
        }
    }

    // Processes incoming order requests
    public void onData(OrderRequest order) {
        if (!isTradingTime) {
            System.out.println("Market closed!! - order rejected");
            return;
        }

        // Handle modify or cancel for queued orders
        if (order.type == RequestType.Modify || order.type == RequestType.Cancel) {
            handleModifyCancel(order);
            return;
        }

        // Throttle orders
        if (ordersThisSecond.get() < maxOrdersPerSecond) { // order allowed
            sendOrder(order);
        } else { // order queued
            orderQueue.add(order);
            System.out.println("Order queued: " + order.orderId);
        }
    }

    //handles modify/cancel requests for queued orders
    private void handleModifyCancel(OrderRequest order) {
        for (OrderRequest queuedOrder : orderQueue) {
            if (queuedOrder.orderId == order.orderId) {
                if (order.type == RequestType.Modify) { //modifying order
                    queuedOrder.price = order.price;
                    queuedOrder.qty = order.qty;
                    System.out.println("Modified queued order: " + order.orderId);
                } else { // cancelling order
                    orderQueue.remove(queuedOrder);
                    System.out.println("Canceled queued order: " + order.orderId);
                }
                return;
            }
        }
        // if no such order found
        System.out.println("Order not found in queue: " + order.orderId);
    }

    //Sends order to exchange with tracking
    private void sendOrder(OrderRequest order) {
        ordersThisSecond.incrementAndGet();
        sentOrderTimes.put(order.orderId, System.nanoTime());
        System.out.println("[Sent] order: " + order.orderId);
    }

    //Processes exchange responses
    public void onData(OrderResponse response) {
        Long sentTime = sentOrderTimes.remove(response.orderId);
        if (sentTime != null) {
            long latency = (System.nanoTime() - sentTime) / 1000; // current time - sent time in microseconds
            logResponse(response, latency);
        }
    }

    // Logs response details to persistent storage
    private void logResponse(OrderResponse response, long latency) {
        String logStr = String.format("Order %d: %s, Latency: %d microseconds%n",
                response.orderId, response.responseType, latency);
        System.out.print(logStr);
    }

    //Processes queued orders each second
    private void processQueue() {
        ordersThisSecond.set(0); // Reset counter at each second

        while (!orderQueue.isEmpty() && ordersThisSecond.get() < maxOrdersPerSecond) {
            OrderRequest order = orderQueue.poll();
            if (order != null) sendOrder(order);
        }
    }

    private void sendLogon() {
        System.out.println("Market Open - Sending LOGON");
    }

    private void sendLogout() {
        System.out.println("Market Closed - Sending LOGOUT");
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    // test
    public static void main(String[] args) throws InterruptedException {
        OrderManagement orderManagement = new OrderManagement();

        // testing orders
        for (int i = 1; i <= 150; i++) {
            OrderRequest order = new OrderRequest();
            order.orderId = i;
            order.type = i%50==0?RequestType.New:RequestType.Cancel;
            orderManagement.onData(order);
            Thread.sleep(10); //simulation of real time flow
        }

        Thread.sleep(2000); //allows system to process
        orderManagement.shutdown();
    }
}