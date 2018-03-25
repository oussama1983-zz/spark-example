import java.time.YearMonth;

public class YearMonthCasualties {

    private YearMonth yearMonth;
    private int numberOfCasualties;

    public YearMonthCasualties(final YearMonth yearMonth, final int numberOfCasualties) {
        this.yearMonth = yearMonth;
        this.numberOfCasualties = numberOfCasualties;
    }

    public YearMonth getYearMonth() {
        return yearMonth;
    }

    public int getNumberOfCasualties() {
        return numberOfCasualties;
    }
}
