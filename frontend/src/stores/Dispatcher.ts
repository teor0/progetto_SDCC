export type Action = {
    type: string;
    payload?: unknown;
};

type Listener = (action: Action) => void;

class Dispatcher {
    private listeners: Listener[] = [];

    register(listener: Listener): () => void {
        this.listeners.push(listener);

        return () => {
            this.listeners = this.listeners.filter(
                (registered) => registered !== listener
            );
        };
    }

    dispatch(action: Action): void {
        for (const listener of this.listeners) {
            listener(action);
        }
    }
}

export const dispatcher = new Dispatcher();