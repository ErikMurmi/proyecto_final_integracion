const functions = require("firebase-functions");
const admin = require("firebase-admin");
// const nodemailer = require("nodemailer");
const {Kafka, Partitioners} = require("kafkajs");
admin.initializeApp();


// const gmailEmail = functions.config().gmail.email;
// const gmailPassword = functions.config().gmail.password;

// Create a Kafka client
const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["pkc-4r087.us-west2.gcp.confluent.cloud:9092"],
    ssl: true,
    sasl: {
        mechanism: "plain",
        username: "IPJC6W4FNIIX2HLP",
        // eslint-disable-next-line max-len
        password: "/698Q1ZII49b8Acy1RL3jeuHnXutbud5o/C+GZoN2YoygJyUOYisxy1PJWHm+Qll",
    },

});

// eslint-disable-next-line max-len
const producer = kafka.producer({createPartitioner: Partitioners.LegacyPartitioner});

exports.onFileUpload = functions.storage.object().onFinalize(async (object) => {
    // const APP_NAME = "IT BACKUP FILES";
    // The Storage bucket that contains the file.
    const fileBucket = object.bucket;
    // File path in the bucket.
    const filePath = object.name;
    // Split the file path into components
    const pathComponents = filePath.split("/");

    // The image name is the last component
    const filename = pathComponents[pathComponents.length - 1];

    // The last folder is the second to last component
    // eslint-disable-next-line max-len
    const lastFolder = pathComponents.length > 1 ? pathComponents[pathComponents.length - 2] : null;

    // File content type.
    const contentType = object.contentType;
    const metageneration = object.metageneration;

    console.log("File change detected, function execution started");
    // console.log("obj: ", object);

    if (filePath.endsWith("/")) {
        console.log("The event was a folder creation ");
        return null;
    }
    if (lastFolder ) {
        console.log(`Metageneration: ${metageneration}`);
        console.log(`File path: ${filePath}`);
        console.log(`Content type: ${contentType}`);

        // Construct the file's public URL
        const publicUrl = `https://firebasestorage.googleapis.com/v0/b/${fileBucket}/o/${encodeURIComponent(filePath)}?alt=media`;
        // Send file info to kafka
        await producer.connect();
        await producer.send({
            topic: "files_backup",
            messages: [
                {value: JSON.stringify({
                    "url": publicUrl,
                    "department": lastFolder,
                })},
            ],
        });
        await producer.disconnect();
    } else {
        const publicUrl = `https://firebasestorage.googleapis.com/v0/b/${fileBucket}/o/${encodeURIComponent(filePath)}?alt=media`;
        await producer.connect();
        await producer.send({
            topic: "files_errors",
            messages: [
                {value: JSON.stringify({
                    "url": publicUrl,
                    "error": `The file ${filename} is not in a folder so it's`+
                    "not possible to notify the specific department." +
                    "Please check it.",
                })},
            ],
        });
        await producer.disconnect();
    }

    console.log("File change detected, function execution completed");
    return null;
});
