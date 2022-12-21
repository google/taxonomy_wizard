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
  * Has Media Taxonomy Data Dictionary stored in Google Sheets and/or Google BigQuery tables.
  * Is a Google Cloud customer.
    * Ability to incur Billing in cloud account.
    * Ability to deploy and run web services.
  * *[Automated Daily Validation only]* Uses Campaign Manager.
    * DV360, SA360, and Google Ads may be supported on request.

### Deployment
1. [Choose an existing](https://cloud.console.google.com/home/dashboard) or [create a new](https://console.cloud.google.com/projectcreate) Google Cloud project.
2. Click the *Cloud Shell* icon ( <span style="background:#DDDDDD;color: #0000FF">&nbsp;>_&nbsp;</span>&nbsp;) in the top right.
3. Clone the Taxonomy Wizard repo:

    ```shell
    git clone https://professional-services.googlesource.com/solutions/taxonomy_wizard
    ```

4. Run the deploy and follow the script prompt:

    ```shell
    ./deploy.sh
    ```
   *(Installs the Taxonomy Wizard Configurator & Validator Cloud resources and updates the Validator sheets plugin code to point to the correct project).*
   * Note the Cloud Function Endpoint.

5. Copy the [Taxonomy Wizard Admin sheet](https://docs.google.com/spreadsheets/d/1whiGO5DfOBBXyMhEnLyCztegrUns-I-fn56EdDzb51o/copy).
    * May require joining the [*taxonomy-wizard-users*](https://groups.google.com/g/taxonomy-wizard-users) Google group.

### Initial Configuration

#### **Admin Console**

*Allows you to configure taxonomies  used for validation.*
1. Copy the *[Taxonomy Wizard - Admin](https://sheets.google.com)* Google Sheet.  <!-- TODO(blevitan): Add link to Google sheet. -->
1. Go to the *Cloud Config* tab of the Admin sheet.
   * Update the **Taxonomy Data Cloud Project ID** (text, most likely) to the project you deployed to.
   * Update the **Cloud Function endpoint** (shown at the end of the deploy script or run `./resources/apps_script/configurator/show_config_endpoint.sh`).
   * You should not need to update the **Taxonomy Data BigQuery Dataset**.
2. Open Apps Script in the Google Sheet (Extensions→Apps Script).
3. On the LHR, click on *Project Settings* (the gear icon <span style="background:#DDDDDD;color: #0000FF">&nbsp;⚙&nbsp;</span>&nbsp;).
4. In the "Google Cloud Platform (GCP) Project" section, click on "Change Project".
5. Change the **Project Number** (numeric, not text) to the same project that you ran the `deploy.sh` script in.

### Sheets Validation Plugin
*Allows you to validate entity names in Google Sheets before creating them in the Google advertising system(s).*
1. Copy the code from the `.js` files in `./resources/apps_script/validator` to an Apps Script project amd deploy it as a plugin to your cloud org.

## Usage Instructions
*Coming Soon...*
<!--TODO(blevitan)-->
