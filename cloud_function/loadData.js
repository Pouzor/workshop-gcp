'use strict';

const google = require('googleapis');
// Get a reference to the Cloud Storage component
const Storage = require('@google-cloud/storage');
const BigQuery = require('@google-cloud/bigquery');

var date = new Date();

const tableId = "bluekai_data";
const datasetId = "workshop";


exports.loadData = function (event, callback) {
    const file = event.data;


    if (file.resourceState === 'exists' && file.metageneration === '1') {
        google.auth.getApplicationDefault(function (err, authClient, projectId) {
            if (err) {
                throw err;
            }

            if (authClient.createScopedRequired && authClient.createScopedRequired()) {
                authClient = authClient.createScoped([
                    'https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/userinfo.email'
                ]);
            }

            const bigquery = BigQuery({
                projectId: projectId
            });

            const storage = Storage({
                projectId: projectId
            });

            bigquery.dataset(datasetId)
                .table(tableId)
                .import(storage.bucket(file.bucket).file(file.name)).then(function(data) {
                    console.log('load data done');
            });
        });
    }
};
