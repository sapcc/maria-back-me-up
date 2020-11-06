let enableButton = function(selector, btnText) {
    const btn = document.getElementById(selector)
    btn.disabled = false
    if (btnText === "") {
        return
    }
    if (btn.getElementsByClassName('spinner-border').length > 0) {
        btn.getElementsByClassName('spinner-border')[0].style.display = "none"
    }
    if (btn.childNodes.length == 1) {
        btn.childNodes[0].nodeValue = btnText
    } else {
        btn.childNodes[2].nodeValue = btnText
    }
}

let disableButton = function(selector, btnText) {
    const btn = document.getElementById(selector)
    btn.disabled = true
    if (btnText === "") {
        return
    }
    btn.getElementsByClassName('spinner-border')[0].style.display = ""
    if (btn.childNodes.length == 1) {
        btn.childNodes[0].nodeValue = btnText
    } else {
        btn.childNodes[2].nodeValue = btnText
    }
}

let clearRestoreModalText = function() {
    const modal = document.getElementById("restoreModal")
    const body = modal.getElementsByClassName("modal-body")[0]
    body.innerHTML = "You are about to restore the database to your selected timestamp. Are you sure?"
}

let updateRestoreModalText = function(text) {
    const update = JSON.parse(text)
    const time = update.time.split(" ")
    const date = new Date(time[0] + " " + time[1])
    const modal = document.getElementById("restoreModal")
    const body = modal.getElementsByClassName("modal-body")[0]
    let html = body.innerHTML
    body.innerHTML = html + "<br>" + date.toLocaleTimeString() + "  " + update.status
}