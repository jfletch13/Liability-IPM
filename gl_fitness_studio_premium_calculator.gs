
## Google Sheets Automation

We also maintain a Google Apps Script to calculate GL and related premiums in Sheets.

- [View Script: gl_premium_calculator.gs](./scripts/gl_premium_calculator.gs)
- Used in: Google Sheet named `Internal Quote Calculator`
- Triggers: `calculateAllPremiums()`


function calculateAllPremiums() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const inputSheet = ss.getSheetByName("inputs");
  const outputSheet = ss.getSheetByName("Output") || ss.insertSheet("Output");
  const lossCostSheet = ss.getSheetByName("health_exercise_clubs");
  const ilfSheet = ss.getSheetByName("ILFs");
  const lcmSheet = ss.getSheetByName("LCM");
  const NIfactorsSheet = ss.getSheetByName("NI Factors");
  const hnoaSheet = ss.getSheetByName("HNOA");
  const cyberSheet = ss.getSheetByName("Cyber");

  const inputData = inputSheet.getDataRange().getValues();
  const headers = inputData[1]; // assume headers in row 2
  const inputRows = inputData.slice(2); // data starts row 3

  const fullHeaders = headers.concat([
    "GL Base Premium",
    "HNOA Premium",
    "DTRP Premium",
    "EPLI Premium",
    "Cyber Premium",
    "Notes"
  ]);

  const lossCostData = lossCostSheet.getRange("A6:C100").getValues();
  const ilfData = ilfSheet.getDataRange().getValues();
  const lcmData = lcmSheet.getDataRange().getValues();
  const hnoaFactorData = hnoaSheet.getRange("I16:K100").getValues();
  const niRevPerEmpData = NIfactorsSheet.getRange("A3:C100").getValues();
  const niGrossSalesData = NIfactorsSheet.getRange("H3:K30").getValues();

  const cyberHeaders = cyberSheet.getRange("C16:Z16").getValues()[0];
  const normalizedCyberHeaders = cyberHeaders.map(h => h?.toString().trim().toUpperCase());
  const cyberBasePremiumRow = cyberSheet.getRange("C18:Z18").getValues()[0];
  const cyberDefenseRow = cyberSheet.getRange("C28:Z28").getValues()[0];

  const lossMap = {};
  lossCostData.forEach(row => {
    const state = row[0]?.toString().trim().toUpperCase();
    if (state) {
      lossMap[state] = { under5000: row[1], over5000: row[2] };
    }
  });

  const ilfMap = {};
  ilfData.forEach(row => {
    const state = row[0]?.toString().trim().toUpperCase();
    const ilf = row[1];
    if (state && typeof ilf === 'number') {
      ilfMap[state] = ilf;
    }
  });

  const lcmMap = {};
  lcmData.forEach(row => {
    const state = row[0]?.toString().trim().toUpperCase();
    const lcm = row[2];
    if (state && typeof lcm === 'number') {
      lcmMap[state] = lcm;
    }
  });

  function getHNOAEmployeeFactor(count) {
    for (let i = 0; i < hnoaFactorData.length; i++) {
      const min = hnoaFactorData[i][0];
      const max = hnoaFactorData[i][1];
      const factor = hnoaFactorData[i][2];
      if (count >= min && count < max) return factor;
    }
    return "";
  }
  function getRevenuePerEmployeeFactor(value) {
  for (let i = 0; i < niRevPerEmpData.length; i++) {
    const from = niRevPerEmpData[i][0];
    const to = niRevPerEmpData[i][1];
    const factor = niRevPerEmpData[i][2];
    if (value >= from && value < to) {
      return (typeof factor === 'number') ? factor : 1;
    }
  }
  return 1; // fallback if no match
}
  function getGrossSalesFactor(value) {
  for (let i = 0; i < niGrossSalesData.length; i++) {
    const from = niGrossSalesData[i][0];
    const to = niGrossSalesData[i][1];
    const factor = niGrossSalesData[i][3]; // Column K = index 3
    if (value >= from && value < to) {
      return (typeof factor === 'number') ? factor : 1;
    }
  }
  return 1; // fallback if no match
}
  function getCyberPremiumForState(stateAbbrev) {
    const state = stateAbbrev?.toString().trim().toUpperCase();
    const index = normalizedCyberHeaders.indexOf(state);
    const defaultIndex = normalizedCyberHeaders.indexOf("DEFAULT");

    let base = null;
    let factor = 1;

    if (state === "HI" && index > -1) {
      base = cyberBasePremiumRow[index];
      factor = cyberDefenseRow[index];
    } else if (defaultIndex > -1) {
      base = cyberBasePremiumRow[defaultIndex];
      factor = cyberDefenseRow[defaultIndex];
    }

    if (typeof base === 'number' && typeof factor === 'number') {
      return Math.round(base * factor);
    }

    return ""; // return blank if lookup fails
  }

  const results = [fullHeaders];

  inputRows.forEach(row => {
    const rowObj = {};
    headers.forEach((h, idx) => rowObj[h] = row[idx]);

    const rawState = rowObj["StateCd"];
    const state = rawState?.toString().trim().toUpperCase();
    const grossSales = rowObj["annualSales"];
    const area = rowObj["area"];
    const employeeCount = rowObj["employeeCount"];

    let notes = [];
    let glPremium = "";
    let hnoaPremium = "";
    let epliPremium = "";
    let cyberPremium = "";
    const dtrpPremium = 0;

    // === GL Premium
    if (state && grossSales && area &&
        lossMap[state] && ilfMap[state] && lcmMap[state] && employeeCount) {
      //const exposureBase = grossSales / 1000;
      const minPayroll = 20000;
      const salesRate = .6667;
      const x1 = grossSales;
      const x2 = (minPayroll*employeeCount)/ salesRate;
      const rawExposure = Math.max(x1,x2);
      const exposureBase= rawExposure/1000;
        //Logger.log("x1 (Gross Sales): " + x1);
        //Logger.log("x2 (Min Payroll × Emp / Sales Rate): " + x2);
        //Logger.log("Exposure Base (raw): " + rawExposure);
        //Logger.log("Exposure Base (÷1000): " + exposureBase);


      const lossCost = area < 5000 ? lossMap[state].under5000 : lossMap[state].over5000;
      const ilf = ilfMap[state];
      const lcm = lcmMap[state];
     
      const revPerEmp = Math.ceil(grossSales / employeeCount);
      const revPerEmpFactor = getRevenuePerEmployeeFactor(revPerEmp);
      const grossSalesFactor = getGrossSalesFactor(grossSales);
       // Logger.log("revPerEmp: " + revPerEmp);
       // Logger.log("revPerEmpFactor: " + revPerEmpFactor);
       // Logger.log("grossSales: " + grossSales);
       // Logger.log("grossSalesFactor: " + grossSalesFactor);
      
      
      const niFactor = revPerEmpFactor * grossSalesFactor;

     // Logger.log("Exposure Base: " + exposureBase);
     // Logger.log("Loss Cost: " + lossCost);
     // Logger.log("ILF + 0.04: " + (ilf + 0.04));
     // Logger.log("LCM: " + lcm);
     // Logger.log("NI Factor: " + niFactor);
     // Logger.log("Expected GL Premium: " +
     

      glPremium = Math.round(exposureBase * lossCost * (ilf + 0.04) * lcm* niFactor);
    } else {notes.push("Missing GL inputs");
    }

    // === HNOA Premium
    if (typeof employeeCount === 'number') {
      const empFactor = getHNOAEmployeeFactor(employeeCount);
      if (empFactor) {
        const ilf = 1.1;
        const ni = 1.0;
        const hired = 35 * ilf * ni * empFactor;
        const nonOwned = 62 * ilf * ni * empFactor;
        hnoaPremium = Math.round(hired + nonOwned);
      } else {
        notes.push("Missing HNOA employee factor");
      }
    } else {
      notes.push("Invalid employee count");
    }

    // === EPLI Premium
    if (typeof employeeCount === 'number') {
      let rate = state === "WA"
        ? (employeeCount <= 25 ? 29 : 13)
        : (employeeCount <= 25 ? 33 : 18);
      const ilf = 0.5;
      const deductibleFactor = 1.0;
      const raw = rate * employeeCount * ilf * deductibleFactor;
      epliPremium = Math.round(Math.max(raw, 50));
    } else {
      notes.push("Invalid employee count for EPLI");
    }

    // === Cyber Premium
    cyberPremium = getCyberPremiumForState(state);
    if (cyberPremium === "") {
      notes.push("Missing Cyber data");
    }

    results.push(row.concat([
      glPremium,
      hnoaPremium,
      dtrpPremium,
      epliPremium,
      cyberPremium,
      notes.join("; ")
    ]));
  });

  outputSheet.clearContents();
  outputSheet.getRange(1, 1, results.length, results[0].length).setValues(results);
}
