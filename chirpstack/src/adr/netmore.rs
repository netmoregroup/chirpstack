use super::{Handler, Request, Response};
use anyhow::Result;
use async_trait::async_trait;
use lrwn::region::MacVersion;
use std::cmp::max;
use std::cmp::min;
use tracing::info;

fn data_rate_to_req_snr(rate: u8) -> f32 {
    match rate {
        5 => -7.5,
        4 => -10.0,
        3 => -12.5,
        2 => -15.0,
        1 => -17.5,
        0 => -20.0,
        _ => unimplemented!(), // Should not happen
    }
}

pub struct Algorithm {}

impl Algorithm {
    pub fn new() -> Self {
        Algorithm {}
    }

    fn required_history_count(&self) -> usize {
        20
    }
    fn get_nb_trans(&self, current_nb_trans: u8, pkt_loss_rate: f32) -> u8 {
        let pkt_loss_table: [[u8; 3]; 4] = [[0, 1, 2], [1, 2, 3], [2, 3, 4], [4, 4, 4]];

        let mut current_nb_trans = current_nb_trans;
        if current_nb_trans < 1 {
            current_nb_trans = 1;
        }

        if current_nb_trans > 3 {
            current_nb_trans = 3;
        }

        let nb_trans_index = current_nb_trans as usize - 1;
        if pkt_loss_rate < 5.0 {
            return pkt_loss_table[0][nb_trans_index];
        } else if pkt_loss_rate < 10.0 {
            return pkt_loss_table[1][nb_trans_index];
        } else if pkt_loss_rate < 30.0 {
            return pkt_loss_table[2][nb_trans_index];
        }

        pkt_loss_table[3][nb_trans_index]
    }

    fn get_packet_loss_percentage(&self, req: &Request) -> f32 {
        if req.uplink_history.is_empty() {
            return 0.0;
        }

        let mut lost_packets: u32 = 0;
        let mut previous_f_cnt: u32 = 0;

        for (i, h) in req.uplink_history.iter().enumerate() {
            if i == 0 {
                previous_f_cnt = h.f_cnt;
                continue;
            }

            lost_packets += h.f_cnt - previous_f_cnt - 1; // there is always an expected difference of 1
            previous_f_cnt = h.f_cnt;
        }

        (lost_packets as f32) / (req.uplink_history.len() as f32) * 100.0
    }

    fn get_max_snr(&self, req: &Request) -> f32 {
        let mut max_snr: f32 = -999.0;
        let end = max(0, req.uplink_history.len() - 5);
        for uh in &req.uplink_history[0..end] {
            if uh.max_snr > max_snr {
                max_snr = uh.max_snr;
            }
        }
        max_snr
    }
}

#[async_trait]
impl Handler for Algorithm {
    fn get_name(&self) -> String {
        "Netmore ADR algorithm".to_string()
    }

    fn get_id(&self) -> String {
        "netmore".to_string()
    }

    async fn handle(&self, req: &Request) -> Result<Response> {
        let mut resp = Response {
            dr: req.dr,
            tx_power_index: req.tx_power_index,
            nb_trans: req.nb_trans,
        };

        // If ADR is disabled, return with current values.
        if !req.adr {
            return Ok(resp);
        }

        // Extract the requested DR and clamp it to Min .. Max in range
        resp.dr = req.dr.clamp(req.min_dr, req.max_dr);
        // Extract the requested power index, and clamp it to Min .. Max in range
        resp.tx_power_index = req
            .tx_power_index
            .clamp(req.min_tx_power_index, req.max_tx_power_index);

        // If DR was changed, cap the power index
        if resp.dr != req.dr {
            resp.tx_power_index = req.min_tx_power_index;
        }

        if (resp.dr != req.dr) || (resp.tx_power_index != req.tx_power_index) {
            resp.nb_trans = 1;
            return Ok(resp);
        };

        // Set the new nb_trans;
        let calc_nb_trans = self.get_nb_trans(req.nb_trans, self.get_packet_loss_percentage(req));

        let snr_max = self.get_max_snr(req);

        if req.uplink_history.len() == self.required_history_count() {
            if calc_nb_trans > resp.nb_trans {
                resp.tx_power_index = req.min_tx_power_index;
            }
            resp.nb_trans = calc_nb_trans;

            let max_nb_trans = if req.skip_fcnt_check { 1 } else { 3 };

            if resp.nb_trans > max_nb_trans {
                resp.dr = max(resp.dr - 1, req.min_dr);
                resp.nb_trans = 1;
            } else {
                if resp.nb_trans < 1 {
                    resp.nb_trans = 1;
                    if resp.dr == req.max_dr {
                        let tx_power_step = match req.mac_version {
                            MacVersion::LORAWAN_1_0_0 | MacVersion::LORAWAN_1_0_1 => 3.0,
                            _ => 2.0,
                        };

                        let snr_min =
                            data_rate_to_req_snr(resp.dr) + req.installation_margin + tx_power_step;
                        if snr_max > snr_min {
                            resp.tx_power_index =
                                min(req.tx_power_index + 1, req.max_tx_power_index);
                        }
                    } else if snr_max > data_rate_to_req_snr(resp.dr + 1) + req.installation_margin
                    {
                        resp.dr += 1;
                        resp.nb_trans = 2;
                        resp.tx_power_index = req.min_tx_power_index;
                    }
                }
                resp.nb_trans = min(resp.nb_trans, max_nb_trans);
            }
        }
        if resp.dr == 0 || resp.dr == 1 {
            resp.nb_trans = 1;
        }

        if resp.dr == 0 && snr_max > data_rate_to_req_snr(1) + req.installation_margin {
            resp.dr = min(1, req.max_dr);
            resp.tx_power_index = req.min_tx_power_index;
        }

        info!(dev_eui = %req.dev_eui,
              current_data_rate = &req.dr,
              requested_data_rate = &resp.dr,
              current_nb_trans = &req.nb_trans,
              requested_nb_trans = &resp.nb_trans,
              current_tx_power = &req.tx_power_index,
              requested_tx_power = &resp.tx_power_index,
              "ADR Request after algorithm");
        Ok(resp)
    }
}
