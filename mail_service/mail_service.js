const {Kafka, Partitioners} = require("kafkajs");
const nodemailer = require("nodemailer");

// Configuración del servidor de correo electrónico
let transporter = nodemailer.createTransport({
    host: "smtp.gmail.com",
    port: 465,
    secure: true,
    auth: {
      user: "erikmurmi@gmail.com",
      pass: "kmxbwbwvbrjyquhi"
    }
});


const run = async () => {
    // Consumidor
    // Create a Kafka client
    const kafka = new Kafka({
        clientId: "mail_service",
        brokers: ["pkc-4r087.us-west2.gcp.confluent.cloud:9092"],
        ssl: true,
        sasl: {
            mechanism: "plain",
            username: "IPJC6W4FNIIX2HLP",
            // eslint-disable-next-line max-len
            password: "/698Q1ZII49b8Acy1RL3jeuHnXutbud5o/C+GZoN2YoygJyUOYisxy1PJWHm+Qll",
        },

    });
    const consumer = kafka.consumer({ groupId: 'mail_service' });
    await consumer.connect();
    await consumer.subscribe({ topic: 'email_notification', fromBeginning: true });

    consumer.run({
        
        eachMessage: async ({ topic, partition, message }) => {
            const message_data = JSON.parse(message.value.toString())
            console.log("msg: ", message_data)
            const empleados = message_data["employees"]
            const recipients = empleados.map((e,index)=> e["work_email"])
            const url = message_data["url"]
            const department = message_data["department"]
            let mailOptions = {
                from: `${department} File Backup Notification <erikmurmi@gmail.com>` ,
                to: recipients,
                subject: `New Document Available for ${department} Department`,
                text: `Dear Team,\n\nWe are pleased to inform you that a new document has been successfully backed up to the cloud storage for the ${department} department. You can access it by following this link: ${url}\n\nBest regards,\n${department} File Backup Team`
            };

            transporter.sendMail(mailOptions, function(error, info) {
                if (error) {
                  console.log(error);
                  res.status(500).send("Error al enviar el correo electrónico");
                } else {
                  console.log("Email sent: " + info.response);
                  res.status(200).send("Correo electrónico enviado");
                }
            });

        },
    });

};

run().catch(console.error);
