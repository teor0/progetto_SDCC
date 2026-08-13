import { Navigate, Route, Routes } from "react-router-dom";
import LoginPage from "./pages/LoginPage";
import RegisterPage from "./pages/RegisterPage";
import GalleriesPage from "./pages/GalleriesPage";
import GalleryDetailsPage from "./pages/GalleryDetailsPage";
import { authStore } from "./stores/AuthStore";

function ProtectedRoute({
                            children,
                        }: {
    children: React.ReactNode;
}) {
    if (!authStore.isAuthenticated()) {
        return <Navigate to="/login" replace />;
    }

    return children;
}

function App() {
    return (
        <Routes>
            <Route path="/register" element={<RegisterPage />}/>
            <Route path="/login" element={<LoginPage />}/>
            <Route
                path="/galleries"
                element={
                    <ProtectedRoute>
                        <GalleriesPage />
                    </ProtectedRoute>
                }
            />

            <Route
                path="/"
                element={<Navigate to="/galleries" replace />}
            />

            <Route
                path="*"
                element={<Navigate to="/galleries" replace />}
            />
            <Route
                path="/galleries/:galleryId"
                element={
                    <ProtectedRoute>
                        <GalleryDetailsPage />
                    </ProtectedRoute>
                }
            />
        </Routes>
    );
}

export default App;