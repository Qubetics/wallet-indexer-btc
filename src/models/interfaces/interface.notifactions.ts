export interface NotificationInterface {
  notification_id?: number;
  message: string;
  amount: number,
  from_user_id: number;
  to_user_id?: number | null;
  notification_type: string;
  tx_id?: number | null;
  tx_type?: string;
  coin_symbol: string | null;
  coin_family: string | null;
  coin_id: number;
  view_status?: string | null;
  state: string | null;
  wallet_address?: string | null;
}
