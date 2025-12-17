// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

let radioBtns = document.querySelectorAll("input[type=radio]")
radioBtns.forEach((elem) => {
    elem.addEventListener("change", function(event) {
        const checked = event.target.checked
        if (event.target.checked) {
            enableButton('download_btn', 'Download')
            enableButton('restore_btn', "Restore")
        }
    })
})

document.querySelector("#restore_btn").addEventListener("click", function() {
    clearRestoreModalText()
})

document.querySelector("#restore_modal_btn").addEventListener("click", function() {
    clearRestoreModalText()
    disableButton('restore_modal_btn', "hang tight")
    postFormData("restore")
    .then(response => {
        if (!response.ok) {
            throw new Error("Not 2xx response")
        }
        const utf8Decoder = new TextDecoder("utf-8");
        const reader = response.body.getReader();
        const stream = new ReadableStream({
            async start(controller) {
                return pump();
                function pump() {
                    return reader.read().then(({ done, value }) => {
                        if (done) {
                            controller.close();
                            enableButton('restore_modal_btn', "Restore")
                            return;
                        }
                        var str = String.fromCharCode.apply(String, value);
                        var lines = str.split('\n');
                        for(var line = 0; line < lines.length; line++){
                            if (lines[line] !== "") {
                                updateRestoreModalText(lines[line])
                            }
                        }
                        return pump();
                    });
                }
            }
        });
    })
    .catch(err => {
        alert("error restoring backup: "+ err)
        enableButton('restore_modal_btn', "Restore")
    })
})

document.querySelector("#download_btn").addEventListener("click", function() {
    disableButton('download_btn', "working on it . . .")
    postFormData("api/restore/download")
    .then(response => {
        if (!response.ok) {
            throw new Error("Not 2xx response")
        }
        return response.blob()
    })
    .then(blob => {
        var url = window.URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = "backup.tar";
        document.body.appendChild(a);
        a.click();    
        a.remove();
        enableButton('download_btn', 'Download')
    })
    .catch(err => {
        enableButton('download_btn', "Download")
        alert("error downloading backup: " + err)
    })
})

let postFormData = function(path) {
    let formData =  new FormData(document.querySelector('form'))
    return fetch(path,
    {
        body: formData,
        method: "post"
    })
}