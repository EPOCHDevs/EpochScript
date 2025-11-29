target_sources(epoch_script PRIVATE
        gap_report.cpp
        base_card_report.cpp
        numeric_card_report.cpp
        boolean_card_report.cpp
        any_card_report.cpp
        index_card_report.cpp
        quantile_card_report.cpp
        table_report.cpp
        bar_chart_report.cpp
        cs_bar_chart_report.cpp
        cs_table_report.cpp
        cs_numeric_card_report.cpp
        pie_chart_report.cpp
        nested_pie_chart_report.cpp
        histogram_chart_report.cpp
        # Line chart reports
        line_chart_report.cpp
        cs_line_chart_report.cpp
        # Area chart reports
        area_chart_report.cpp
        cs_area_chart_report.cpp
        # Heatmap reports
        heatmap_report.cpp
        cs_heatmap_report.cpp
        # Boxplot reports
        boxplot_report.cpp
        cs_boxplot_report.cpp
        # Numeric line chart reports
        numeric_line_chart_report.cpp
        # XRange chart reports
        xrange_chart_report.cpp
        cs_xrange_chart_report.cpp)
