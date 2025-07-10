import BigNumber from "bignumber.js";
import { dbHelper } from ".";
import { NotificationModel } from "../models/tables";
import firebaseAdmin from 'firebase-admin';
import { fcmServiceAccount } from "../constants/firebase.admin.key";
import { Op } from "sequelize";
import { NotificationInterface } from "../models/interfaces";


class UtilityHelper {
  public messaging: firebaseAdmin.messaging.Messaging;

  constructor() {
    (async () => {
      firebaseAdmin.initializeApp({
        credential: firebaseAdmin.credential.cert(fcmServiceAccount),
      });
      this.messaging = firebaseAdmin.messaging();
    })();
  }

  bigNumberSafeConversion = async (val: number) => {
    const amount = val.toString();
    const value = new BigNumber(amount);
    return value.toFixed();
  };

  public async sendNotification(data: any) {
    try {
      const toUserId: number = data.to_user_id ? data.to_user_id : 0;
      const deviceTokens: any = await dbHelper.getDeviceTokens(toUserId, data.wallet_address, data.coin_family);
      // console.log('deviceTokens >>>', deviceTokens);
      let finalDeviceTokens: any = [];

      for await (let deviceToken of deviceTokens) {
        finalDeviceTokens.push(deviceToken.device_token);
      }

      if (finalDeviceTokens?.length > 0) {
        const announcement_title: string =
          data.announcement_title == undefined ? "" : data.announcement_title;
        const announcement_message: string =
          data.announcement_message == undefined
            ? ""
            : data.announcement_message;

        const notification = {
          title: data.title,
          body: data.message,
        };

        this.messaging
          .sendEachForMulticast({
            tokens: finalDeviceTokens,
            notification: notification,
            data: {
              body: data.message ? data.message : "",
              title: data.title ? data.title : "",
              notification_type: data.notification_type ? data.notification_type : "",
              tx_id: data.tx_id ? data.tx_id.toString() : "0",
              tx_type: data.tx_type ? data.tx_type : "",
              from_user_id: data.from_user_id ? data.from_user_id.toString() : "0",
              user_coin_id: data.coin_id ? data.coin_id?.toString() : "0",
              announcement_title: announcement_title,
              announcement_message: announcement_message,
            }
          })
          .then((response: any) => {
            console.log("💥🚀 ~ firebase admin push notification", JSON.stringify(response));
          })
          .catch((error: any) => {
            console.error(
              "💥 ~ firebase admin sending notification error",
              error
            );
          });
      }

      /* save notification */
      const checkOldNotif: any = await NotificationModel.count({
        where: {
          wallet_address: { [Op.like]: data.wallet_address },
          notification_type: data.notification_type,
          tx_id: data.tx_id,
        },
      });
      if (checkOldNotif == 0) {
        const notificationData: NotificationInterface = {
          message: data.message,
          from_user_id: data.from_user_id ? data.from_user_id : 0,
          amount: data.amount,
          to_user_id: toUserId,
          coin_symbol: data.coin_symbol,
          notification_type: data.notification_type,
          tx_id: data.tx_id,
          coin_id: data.coin_id,
          coin_family: data.coin_family,
          wallet_address: data.wallet_address,
          tx_type: data.tx_type,
          view_status: "0",
          // state: "0",
        };
        await NotificationModel.create(notificationData);
      }
      return true;
    } catch (error) {
      console.error(`🔥 ~ sendNewNotification utility.helper.ts error`, error);
      return false;
    }
  }
}
export let utilityHelper = new UtilityHelper();
