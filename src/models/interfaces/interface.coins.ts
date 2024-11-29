import { BooleanEnum, TokenStandard } from "../../enum";

export interface CoinInterface {
    coin_id: number;
    coin_name: string;
    coin_symbol: string;
    coin_gicko_alias: string | null;
    coin_image?: string | null;
    coin_family: number;
    coin_status: BooleanEnum | 0;
    is_token: BooleanEnum | 0;
    token_type: TokenStandard;
    decimals: number | 0;
    cmc_id?: number | 0;
    is_on_cmc?: number | 1;
    usd_price?: number | 1;
    withdraw_limit?: number | 0;
    token_abi?: string | null;
    uuid?: string | null;
    token_address?: string | null;
    for_swap?: number;
    added_by?: string;
    created_at?: string;
    updated_at?: string;
}
