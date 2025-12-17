// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

let stoppingBackup = false
document.querySelector("#inc_backup_btn").addEventListener("click", function() {
    disableButton("inc_backup_btn", "creating backup...")
    fetch("api/backup/inc/create",
    {
        method: "GET"
    })
    .then(async function(res) {
        if (!res.ok) {
             err = await res.text()
             throw new Error(err)
        }
        enableButton("inc_backup_btn", "Trigger Incremental Backup")
    })
    .catch(err => {
        alert(err)
        enableButton("inc_backup_btn", "Trigger Incremental Backup")
    })
})

document.querySelector("#backup_switch").addEventListener("change", function(ev) {
    if (ev.target.checked) {
        getStartStopBackup("start")
        .catch(err => {
            alert(err)
            document.querySelector("#backup_switch").checked = false
        })
    } else {
        stoppingBackup = true
        disableButton("backup_switch", "")
        getStartStopBackup("stop")
        .then(res => {
            enableButton("backup_switch", "")
            stoppingBackup = false
        })
        .catch(err => {
            enableButton("backup_switch", "")
            stoppingBackup = false
            document.querySelector("#backup_switch").checked = false
            alert(err)
        })
    }
})

function getStartStopBackup(action) {
    return fetch("api/backup/" + action,
    {
        method: "GET"
    })
    .then(async function(res) {
        if (!res.ok) {
             throw new Error("cannot start/stop backup")
        }
        return res
    })
    .catch(err => {
        throw new Error("cannot connect to backup api")
    })
}

function getBackupStatusSocket(){
    let loc = window.location
    let uri = 'ws:'
    let colorOff = '#ed143d'
    let colorOn = '#2fb32f'

    if (loc.protocol === 'https:') {
        uri = 'wss:'
    }
    uri += '//' + loc.host
    uri += loc.pathname + 'api/backup/status'

    ws = new WebSocket(uri)

    ws.onopen = function() {
        console.log('Connected')
    }
    ws.onclose = function(ev) {
        document.querySelectorAll('.backup_sts').forEach(el => {
            el.childNodes[0].nodeValue = 'ERROR: conn closed, reason:' + ev.code;
            el.style.backgroundColor = 'colorOff'
            document.querySelector("#backup_switch").checked = false
        })
    }
    ws.onmessage = function(evt) {
        let sts = JSON.parse(evt.data)
        let health = sts.health
        if (sts.active && !stoppingBackup) {
            document.querySelector("#backup_switch").checked = true
            enableButton("inc_backup_btn", "")
        } else if (!stoppingBackup) {
            document.querySelector("#backup_switch").checked = false
            disableButton("inc_backup_btn", "")
        }
        for (let storage in health.FullBackup) {
            let color = health.FullBackup[storage] == 0 ? colorOff : colorOn
            document.getElementById(storage + "_full").style.backgroundColor = color
        }
        for (let storage in health.IncBackup) {
            let color = health.IncBackup[storage] == 0 ? colorOff : colorOn
            document.getElementById(storage + "_inc").style.backgroundColor = color
        }
    }
} 