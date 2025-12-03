//
// Created by adesola on 4/8/25.
//

#pragma once
#include <filesystem>
#include <functional>
#include <yaml-cpp/yaml.h>
#include <epoch_core/enum_wrapper.h>
#include <string>
#include <unordered_set>

// Card selector enums (defined here to avoid circular dependencies)
CREATE_ENUM(CardRenderType,
            Text,        // Generic text/label
            Integer,     // Integer numeric value
            Decimal,     // Decimal/floating point numeric value
            Percent,     // Percentage value
            Monetary,    // Currency/money value
            Duration,    // Duration in nanoseconds
            Badge,       // Badge/pill element
            Timestamp,   // Date/time display
            Boolean);    // True/False indicator

CREATE_ENUM(CardSlot,
            PrimaryBadge,   // Top-left badge
            SecondaryBadge, // Top-right badge
            Hero,           // Center large element
            Subtitle,       // Below hero
            Footer,         // Bottom
            Details);       // "Show More" expandable section

// Color enum - maps to frontend color schemes (Tailwind, shadcn/ui, etc.)
// Semantic colors are context-aware, real colors are explicit
CREATE_ENUM(Color,
            // Semantic Colors (context-aware)
            Default,     // Neutral/gray - default UI color
            Primary,     // Brand/primary color
            Secondary,   // Secondary brand color
            Success,     // Green - success states
            Warning,     // Yellow/orange - warning states
            Error,       // Red - error/danger states
            Info,        // Blue - informational states
            Muted,       // Muted/subdued color
            Accent,      // Accent color for highlights

            // Grayscale Spectrum
            Slate,       // Cool gray (Tailwind: slate)
            Gray,        // Neutral gray (Tailwind: gray)
            Zinc,        // Warm gray (Tailwind: zinc)
            Neutral,     // True gray (Tailwind: neutral)
            Stone,       // Warm beige-gray (Tailwind: stone)
            Black,       // Pure black
            White,       // Pure white

            // Cool Colors
            Blue,        // Standard blue
            Sky,         // Light blue (Tailwind: sky)
            Cyan,        // Cyan/aqua
            Teal,        // Teal (blue-green)
            Indigo,      // Indigo (deep blue)
            Violet,      // Violet (blue-purple)
            Purple,      // Purple

            // Warm Colors
            Red,         // Standard red
            Rose,        // Pink-red (Tailwind: rose)
            Pink,        // Pink
            Fuchsia,     // Magenta-pink (Tailwind: fuchsia)
            Orange,      // Orange
            Amber,       // Amber (orange-yellow, Tailwind: amber)
            Yellow,      // Yellow
            Lime,        // Lime green (Tailwind: lime)

            // Green Spectrum
            Green,       // Standard green
            Emerald,     // Emerald green (Tailwind: emerald)

            // Metallic/Special
            Gold,        // Gold
            Silver,      // Silver
            Bronze);     // Bronze

// Unified Icon enum - consolidates CardIcon and FlagIcon for consistency
// All icons map to Lucide icons (https://lucide.dev/icons)
// Note: Old enum values normalized: TrendUp->TrendingUp, Dollar->DollarSign, Candle->CandlestickChart
CREATE_ENUM(Icon,
            // Charts & Analysis
            BarChart,         // Bar chart (Lucide: BarChart)
            BarChart2,        // Bar chart variant (Lucide: BarChart2)
            BarChart3,        // Bar chart variant (Lucide: BarChart3)
            Chart,            // General analysis/charts (Lucide: BarChart3) [legacy name]
            LineChart,        // Line chart (Lucide: LineChart)
            AreaChart,        // Area chart (Lucide: AreaChart)
            PieChart,         // Pie chart (Lucide: PieChart)
            CandlestickChart, // Candlestick chart (Lucide: CandlestickChart)
            Activity,         // Activity/pulse (Lucide: Activity)
            TrendingUp,       // Trending up (Lucide: TrendingUp)
            TrendingDown,     // Trending down (Lucide: TrendingDown)

            // Financial & Money
            DollarSign,       // Dollar sign (Lucide: DollarSign)
            Euro,             // Euro sign (Lucide: Euro)
            PoundSterling,    // Pound sign (Lucide: PoundSterling)
            Bitcoin,          // Bitcoin (Lucide: Bitcoin)
            CreditCard,       // Credit card (Lucide: CreditCard)
            Wallet,           // Wallet (Lucide: Wallet)
            Coins,            // Coins (Lucide: Coins)
            Banknote,         // Banknote (Lucide: Banknote)
            Calculator,       // Calculator (Lucide: Calculator)
            Percent,          // Percent (Lucide: Percent)

            // Documents & Files
            FileText,         // File with text (Lucide: FileText)
            File,             // Generic file (Lucide: File)
            Files,            // Multiple files (Lucide: Files)
            Receipt,          // Receipt (Lucide: Receipt)
            Newspaper,        // Newspaper (Lucide: Newspaper)
            BookOpen,         // Book (Lucide: BookOpen)
            Clipboard,        // Clipboard (Lucide: Clipboard)

            // Alerts & Notifications
            Bell,             // Bell (Lucide: Bell)
            BellRing,         // Bell ringing (Lucide: BellRing)
            AlertCircle,      // Alert circle (Lucide: AlertCircle)
            AlertTriangle,    // Alert triangle (Lucide: AlertTriangle)
            AlertOctagon,     // Alert octagon (Lucide: AlertOctagon)
            Info,             // Info (Lucide: Info)
            HelpCircle,       // Help circle (Lucide: HelpCircle)
            MessageCircle,    // Message (Lucide: MessageCircle)

            // Actions & Signals
            Signal,           // Signal/zap (Lucide: Zap)
            Zap,              // Zap (Lucide: Zap)
            Play,             // Play (Lucide: Play)
            Pause,            // Pause (Lucide: Pause)
            Square,           // Square/stop (Lucide: Square)
            Flag,             // Flag (Lucide: Flag)
            Bookmark,         // Bookmark (Lucide: Bookmark)
            Target,           // Target (Lucide: Target)

            // Arrows & Directions
            ArrowUp,          // Arrow up (Lucide: ArrowUp)
            ArrowDown,        // Arrow down (Lucide: ArrowDown)
            ArrowLeft,        // Arrow left (Lucide: ArrowLeft)
            ArrowRight,       // Arrow right (Lucide: ArrowRight)
            ArrowLeftRight,   // Arrow left-right (Lucide: ArrowLeftRight)
            ArrowUpDown,      // Arrow up-down (Lucide: ArrowUpDown)
            ChevronsUp,       // Chevrons up (Lucide: ChevronsUp)
            ChevronsDown,     // Chevrons down (Lucide: ChevronsDown)
            MoveUp,           // Move up (Lucide: MoveUp)
            MoveDown,         // Move down (Lucide: MoveDown)

            // Status & Checks
            CheckCircle,      // Check circle (Lucide: CheckCircle)
            Check,            // Check (Lucide: Check)
            X,                // X/close (Lucide: X)
            XCircle,          // X circle (Lucide: XCircle)
            Circle,           // Circle (Lucide: Circle)
            CircleDot,        // Circle with dot (Lucide: CircleDot)
            Minus,            // Minus (Lucide: Minus)
            Plus,             // Plus (Lucide: Plus)

            // Trading & Markets
            Trade,            // Trade (Lucide: ArrowLeftRight) [legacy name]
            Position,         // Position (Lucide: Layers) [legacy name]
            Layers,           // Layers (Lucide: Layers)
            Split,            // Split (Lucide: Split)
            Shuffle,          // Shuffle (Lucide: Shuffle)
            Repeat,           // Repeat (Lucide: Repeat)
            RefreshCw,        // Refresh (Lucide: RefreshCw)

            // Time & Calendar
            Calendar,         // Calendar (Lucide: Calendar)
            CalendarDays,     // Calendar with days (Lucide: CalendarDays)
            Clock,            // Clock (Lucide: Clock)
            Timer,            // Timer (Lucide: Timer)
            Hourglass,        // Hourglass (Lucide: Hourglass)

            // Data & Database
            Database,         // Database (Lucide: Database)
            Table,            // Table (Lucide: Table)
            Filter,           // Filter (Lucide: Filter)
            Search,           // Search (Lucide: Search)
            Download,         // Download (Lucide: Download)
            Upload,           // Upload (Lucide: Upload)

            // Settings & Tools
            Settings,         // Settings (Lucide: Settings)
            Wrench,           // Wrench (Lucide: Wrench)
            Sliders,          // Sliders (Lucide: Sliders)
            Edit,             // Edit (Lucide: Edit)
            Copy,             // Copy (Lucide: Copy)
            Trash,            // Trash (Lucide: Trash)

            // Shapes & UI
            Box,              // Box (Lucide: Box)
            Package,          // Package (Lucide: Package)
            Folder,           // Folder (Lucide: Folder)
            Star,             // Star (Lucide: Star)
            Heart,            // Heart (Lucide: Heart)
            Eye,              // Eye (Lucide: Eye)
            EyeOff,           // Eye off (Lucide: EyeOff)

            // People & Users
            User,             // User (Lucide: User)
            Users,            // Users (Lucide: Users)
            UserPlus,         // User plus (Lucide: UserPlus)
            UserMinus,        // User minus (Lucide: UserMinus)

            // Communication
            Mail,             // Mail (Lucide: Mail)
            Phone,            // Phone (Lucide: Phone)
            Send,             // Send (Lucide: Send)
            Share,            // Share (Lucide: Share)

            // Miscellaneous
            Globe,            // Globe (Lucide: Globe)
            Map,              // Map (Lucide: Map)
            MapPin,           // Map pin (Lucide: MapPin)
            Lock,             // Lock (Lucide: Lock)
            Unlock,           // Unlock (Lucide: Unlock)
            Shield,           // Shield (Lucide: Shield)
            Award,            // Award (Lucide: Award)
            Gift,             // Gift (Lucide: Gift)
            Sparkles);        // Sparkles (Lucide: Sparkles)

namespace epoch_script {
constexpr auto ARG = "SLOT";
constexpr auto ARG0 = "SLOT0";
constexpr auto ARG1 = "SLOT1";
constexpr auto ARG2 = "SLOT2";
constexpr auto ARG3 = "SLOT3";
constexpr auto RESULT = "result";

// Cross-sectional report key
constexpr auto GROUP_KEY = "ALL";

// Polygon data source transform IDs
namespace polygon {
constexpr auto BALANCE_SHEET = "balance_sheet";
constexpr auto INCOME_STATEMENT = "income_statement";
constexpr auto CASH_FLOW = "cash_flow";
constexpr auto FINANCIAL_RATIOS = "financial_ratios";
constexpr auto QUOTES = "quotes";
constexpr auto TRADES = "trades";
constexpr auto AGGREGATES = "aggregates";
constexpr auto COMMON_INDICES = "common_indices";
constexpr auto INDICES = "indices";

// Reference Stocks
constexpr auto COMMON_REFERENCE_STOCKS = "common_reference_stocks";
constexpr auto REFERENCE_STOCKS = "reference_stocks";

// FX Pairs
constexpr auto COMMON_FX_PAIRS = "common_fx_pairs";
constexpr auto FX_PAIRS = "fx_pairs";

// Crypto Pairs
constexpr auto COMMON_CRYPTO_PAIRS = "common_crypto_pairs";
constexpr auto CRYPTO_PAIRS = "crypto_pairs";

constexpr auto NEWS = "news";
constexpr auto DIVIDENDS = "dividends";
constexpr auto SPLITS = "splits";
constexpr auto TICKER_EVENTS = "ticker_events";
constexpr auto SHORT_INTEREST = "short_interest";
constexpr auto SHORT_VOLUME = "short_volume";

// Set of all Polygon transform IDs for easy contains checks
inline const std::unordered_set<std::string> ALL_POLYGON_TRANSFORMS = {
    BALANCE_SHEET,
    INCOME_STATEMENT,
    CASH_FLOW,
    FINANCIAL_RATIOS,
    QUOTES,
    TRADES,
    AGGREGATES,
    COMMON_INDICES,
    INDICES,
    COMMON_REFERENCE_STOCKS,
    REFERENCE_STOCKS,
    COMMON_FX_PAIRS,
    FX_PAIRS,
    COMMON_CRYPTO_PAIRS,
    CRYPTO_PAIRS,
    NEWS,
    DIVIDENDS,
    SPLITS,
    TICKER_EVENTS,
    SHORT_INTEREST,
    SHORT_VOLUME
};
} // namespace polygon

// FRED (Federal Reserve Economic Data) transform IDs
namespace fred {
constexpr auto ECONOMIC_INDICATOR = "economic_indicator";

// Set of all FRED transform IDs for easy contains checks
inline const std::unordered_set<std::string> ALL_FRED_TRANSFORMS = {
    ECONOMIC_INDICATOR
};
} // namespace fred

using FileLoaderInterface = std::function<YAML::Node(std::string const &)>;
using AIGeneratedStrategiesLoader = std::function<std::vector<std::string>()>;
} // namespace epoch_script