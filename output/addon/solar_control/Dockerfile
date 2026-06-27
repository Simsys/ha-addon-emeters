ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM $BUILD_FROM

# Benötigte Laufzeit-Abhängigkeiten installieren (falls nötig)
# RUN apk add --no-cache ca-certificates

# Kopiere die vorompilierte Rust-Binary in den Container
COPY root/solar_control /usr/bin/solar_control
RUN chmod a+x /usr/bin/solar_control

# Startbefehl für das Add-on
CMD [ "/usr/bin/solar_control" ]
