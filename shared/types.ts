export interface Order {
  orderId: string;
  customerId: string;
  items: { sku: string; qty: number }[];
  total: number;
  status: string;
}

export interface Payment {
  orderId: string;
  items: { sku: string; qty: number }[];
  amount: number;
  status: "succeeded" | "failed";
  processedAt: string;
}

export interface InventoryReservation {
  orderId: string;
  items: { sku: string; qty: number }[];
  reserved: boolean;
  reservedAt: string;
}

export interface Shipment {
  orderId: string;
  trackingId: string;
  shippedAt: string;
}
