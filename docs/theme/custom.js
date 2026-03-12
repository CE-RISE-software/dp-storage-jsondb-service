(function () {
    function setFavicon() {
        var icon = document.querySelector("link[rel~='icon']");
        if (!icon) {
            icon = document.createElement("link");
            icon.rel = "icon";
            document.head.appendChild(icon);
        }
        icon.href = "favicon.png";
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", setFavicon);
    } else {
        setFavicon();
    }
})();
