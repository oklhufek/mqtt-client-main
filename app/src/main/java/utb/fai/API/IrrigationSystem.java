package utb.fai.API;

/**
 * Rozhrani pro komunikaci se zavlazovacim systemem
 * 
 * ZDE NEMENIT ZADNOU CAST KODU
 */
public interface IrrigationSystem {

    /**
     * Navrati stav o tom zda je zavlazovaci system v aktualni chvili aktivni
     * 
     * @return True v pripade kdy je zavlazovaci system aktivni
     */
    public boolean isActive();

    /*
     * Aktivuje zavlazovaci system
     */
    public void activate();

    /**
     * Deaktivuje zavlazovaci system
     */
    public void deactivate();

    /**
     * Tato metoda navrati stav o to zda doslo k nejake poruce zavlazovaciho
     * zarizeni
     * 
     * @return True v pripade vyskytu nejake poruchy
     */
    public boolean hasFault();
}
