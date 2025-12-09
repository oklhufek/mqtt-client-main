# Testovací MQTT Klient

Tento projekt obsahuje testovacího MQTT klienta, který umožňuje uživatelům dynamicky spravovat připojení k MQTT brokeru, odebírat témata, odhlašovat odběr a odesílat zprávy na specifická témata pomocí jednoduchých textových příkazů. Nástroj byl vytvořen jako pomucká pro výuku v předmětu programování síťových aplikací na UTB FAI.

## Funkce

- **Dynamické nastavení URL MQTT brokera:** URL brokera je vyžádáno od uživatele při spuštění aplikace.
- **Odebírání a odhlašování odběru tématických okruhů:** Umožňuje uživatelům odebírat a odhlašovat odběr tématických okruhů v reálném čase.
- **Odesílání zpráv:** Umožňuje uživatelům odesílat zprávy na specifická témata.
- **Zobrazení seznamu odebíraných tématických okruhů.**

## Požadavky

Pro spuštění tohoto nástroje je potřeba mít nainstalované:

- Java JDK 17 nebo novější
- Vyžaduje přístup k MQTT brokeru (např. Mosquitto, HiveMQ)

## Nastavení a spuštění

1. **Nainstalujte MQTT Broker:** Pokud ještě nemáte nainstalovaný MQTT broker, stáhněte a nainstalujte například Mosquitto z [oficiálních stránek](https://mosquitto.org/download/).

2. **Sestavení aplikace:** Sestavte aplikaci pomocí příkazu `gradle build` v kořenovém adresáři projektu.

3. **Spuštění aplikace:** Spusťte aplikaci pomocí příkazu `java -jar build/libs/app-name.jar`. Nahraďte `app-name.jar` názvem sestaveného JAR souboru.

Po spuštění aplikace budete vyzváni k zadání URL MQTT brokera (včetně portu). Následně můžete interaktivně používat podporované příkazy.

## Příkazy

- **/set-url <broaker-url>** - Nastaví URL brokeru. Při změně tohoto parametru už za běhu aplikace dojde automaticky k přepojení.
- **/set-client-id <client-id>** - Nastaví ID klienta a znovu se připojí k brokeru. Při změně tohoto parametru už za běhu aplikace dojde automaticky k přepojení.
- **/set-subscribe-topic <topic>** - Nastaví odeběr zadaného tématu.
- **/unsubscribe-topic <topic>** - Odhlásí odběr zadaného téma.
- **/list-topics** - Zobrazí seznam všech odebíraných tématických okruhů.
- **/send-message <topic> <message>** - Odešle zprávu na zadané téma.
- **/help** - Zobrazí nápovědu s dostupnými příkazy.
- **/quit** - Ukončí program.
