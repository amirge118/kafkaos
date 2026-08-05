export interface Refund {
  orderId: string;
  reason: string;
  status: "issued";
  issuedAt: string;
}
