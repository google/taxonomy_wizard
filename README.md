# Taxonomy Wizard
## Overview
Taxonomy Wizard provides pre-creation **validation** and post-creation **enforcement** of customer media taxonomy naming conventions in Google's advertising products.

E.g., If an advertisers campaign names need to follow this convention:
		`<YYYYQ#>_<CampaignType>_<TargetingGroup>_<FreeformText>`

The tool will:
  * <span style="color: #DD0000">Flag</span> a campaign named "**<span style="color: #DD0000">Q42022</span>**_brandbuilding_millennial_WinterCampaign"
  * <span style="color: #009900">Allow</span> a campaign named "<span style="color: #009900">2022Q4</span>_brandbuilding_millennial_FallCampaign"

## Installation

### Prereqs
  * Access to gSuite (Sheets).
  * Has Media Taxonomy Data Dictionary stored in Google Sheets (stored in Google BigQuery tables directly supported on request).
  * Is a Google Cloud customer.
    * Ability to incur Billing in cloud account.
    * Ability to deploy and run web services.
  * *[Automated Daily Validation only]* Uses Campaign Manager.
    * DV360, SA360, and Google Ads may be supported on request.

### Deployment
1. [Choose an existing](https://console.cloud.google.com/home/dashboard)
 or [create a new](https://console.cloud.google.com/projectcreate)
 Google Cloud project (e.g., `taxonomy-wizard-yourcompanyname`).
2. Click the *Cloud Shell* icon ( <span style="background:#DDDDDD;color: #0000FF">&nbsp;>_&nbsp;</span>&nbsp;) in the top right.
3. Clone the Taxonomy Wizard repo:

    ```shell
    git clone https://professional-services.googlesource.com/solutions/taxonomy_wizard
    ```

4. Run the deploy and follow the script prompt:

    ```shell
    ./deploy.sh
    ```
   *(Installs the Taxonomy Wizard [Configurator](./resources/python_cloud_functions/configurator/) & [Validator](./resources/python_cloud_functions/validator/) Cloud resources and updates the Sheets [Validator plugin code](./resources/apps_script/validator/) to point to the correct project).*
   * Note the *Project Id*, *Project Number* and *Configurator Cloud Function Endpoint* shown at script completion.

5. Copy the [Taxonomy Wizard Admin sheet](https://docs.google.com/spreadsheets/d/1whiGO5DfOBBXyMhEnLyCztegrUns-I-fn56EdDzb51o/copy)
.
    * May require joining the [*taxonomy-wizard-users*](https://groups.google.com/g/taxonomy-wizard-users)
 Google group.

### Initial Configuration

#### **Admin Console**

*Allows you to configure taxonomies  used for validation.*
1. If needed, run [this script](./resources/apps_script/configurator/show_manual_inputs.sh) to display the *Project Id*, *Project Number* and *Cloud Function Endpoint*.
2. Go to the *Cloud Config* tab of the Admin sheet copied at the end of *Deployment*.
   * Copy the *Project Id* to **Taxonomy Data Cloud Project ID**.
   * Copy the the *Configurator Cloud Function Endpoint* to **Cloud Function Endpoint**. Configurator cloud function endpoint.
   * (You should not need to update the **Taxonomy Data BigQuery Dataset**.)
3. Open Apps Script in the Google Sheet (Extensions→Apps Script).
4. On the LHR, click on *Project Settings* (the gear icon <span style="background:#DDDDDD;color: #0000FF">&nbsp;⚙&nbsp;</span>&nbsp;).
5. In the "Google Cloud Platform (GCP) Project" section, click on "Change Project".
6. Copy the *Project Number* to **Project Number**.

### Sheets Validation Plugin
*Allows you to validate entity names in Google Sheets before creating them in the Google advertising system(s).*
1. Copy the code from the `.js` files in `./resources/apps_script/validator` to an Apps Script project.
2. [Publish](https://developers.google.com/workspace/marketplace/how-to-publish
 it **PRIVATELY** (to your company's org ONLY) as an add-on.

## Usage Instructions
*Coming Soon...*
<!--TODO: Implement.-->
## Disclaimer
*This is **NOT** an officially supported Google product.*
