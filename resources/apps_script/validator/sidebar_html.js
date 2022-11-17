/**  Copyright 2022 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

const SIDEBAR_HTML_PRE = 
`<!DOCTYPE html>
<html>
  <head>
    <link rel="stylesheet" type="text/css" href="https://bootswatch.com/3/paper/bootstrap.min.css">
    <style>
      body,html {
        height:100%;
        overflow:hidden;
      }
      #header {
        width: 100%;
      }
      #footer {
        width: 100%;
      }
      #list {
        top: 60px;
        bottom: 75px;
        overflow-x: hidden;
        overflow-y: scroll;
        width: 100%;
      }
      ul {
        padding-left: 10px;
      }
      .radio label {
          padding-left: 25px;
      }
      input[type="radio"] {
          margin: 4px 0 0;
      }
      .radio input[type="radio"] {
          -webkit-appearance: none;
          margin-left: -25px;
      }
      .radio input[type="radio"]:focus {
          outline: none;
      }
      .radio input[type="radio"]:after {
          content: '';
          display: block;
          height: 18px;
          transition: 240ms;
          width: 18px;
      }
      .radio input[type="radio"]:checked:before {
          -webkit-transform: rotate(45deg);
          transform: rotate(45deg);
          position: absolute;
          left: 6px;
          top: 0;
          display: table;
          width: 6px;
          height: 12px;
          border: 2px solid #fff;
          border-top: 0;
          border-left: 0;
          content: '';
      }
      .radio input[type="radio"]:checked:after {
          background-color: #0c84e4;
          border-color: #0c84e4;
      }
      .hidden_item {
          display: none;
      }
    </style>
  </head>
  <body id='fields'>
    <div id='header'>
      <h6 style='margin-left:10px;margin-right:10px;'>Choose Taxonomy to validate against:</h6>
    </div>
    <div id='list'>
      <ul class="list">`;

const SIDEBAR_HTML_POST =
`      </ul>
    </div>
    <div id='footer'>
      <a id='getDataButton' href='#' class='btn btn-primary' onclick='validateNamesInCells()' style='margin-top:15px;margin-left:10px;'>Validate Selected Cells</a>
    </div>
    <script type="text/javascript">
      function $(id) {
        return document.getElementById(id);
      }
      
      function validateNamesInCells() {
        $('getDataButton').classList.add('disabled');
        $('getDataButton').textContent ='Validating...';
        
        const selected = document.querySelector('.radio input[type="radio"]:checked').value;
        console.log('run');

        google.script.run
        .withFailureHandler(function(e) {
          // Any runtime errors have already been alerted by the spreadsheet script.
          $('getDataButton').classList.remove('disabled');
          $('getDataButton').textContent = 'Validate Selected Cells (err)';
        })
        .withSuccessHandler(function(r) {
          // Any data errors have already been alerted by the spreadsheet script.
          $('getDataButton').classList.remove('disabled');
          $('getDataButton').textContent = 'Validate Selected Cells';
        })
        .validateNamesInCells(selected);
      }
    </script>
  <body>
</html>
`;