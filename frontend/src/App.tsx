import { Navigate, Route, Routes, Outlet } from "react-router-dom";
import LoginPage from "./pages/LoginPage";
import RegisterPage from "./pages/RegisterPage";
import GalleriesPage from "./pages/GalleriesPage";
import GalleryDetailsPage from "./pages/GalleryDetailsPage";
import NotificationFeed from "./components/NotificationFeed";
import { authStore } from "./stores/AuthStore";

function ProtectedLayout() {
    if (!authStore.isAuthenticated()) {
        return <Navigate to="/login" replace />;
    }
    return (
        <>
            <NotificationFeed />
            <Outlet />
        </>
    );
}

function App() {
    return (
        <Routes>
            <Route path="/register" element={<RegisterPage />} />
            <Route path="/login" element={<LoginPage />} />

            <Route element={<ProtectedLayout />}>
                <Route path="/galleries" element={<GalleriesPage />} />
                <Route path="/galleries/:galleryId" element={<GalleryDetailsPage />} />
            </Route>

            <Route path="/" element={<Navigate to="/galleries" replace />} />
            <Route path="*" element={<Navigate to="/galleries" replace />} />
        </Routes>
    );
}

export default App;